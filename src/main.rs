//main.rs
mod config;
mod format;
mod hardware;
mod storage;
mod engine;
mod daemon;
mod database;
mod gateway;

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::sync::mpsc;
use log::{info, error};
use clap::Parser;

use config::*;
use hardware::*;
use engine::*;
use daemon::*;
use database::*;
use gateway::*;

fn main() {
    let cli = Cli::parse();
    let use_direct_io = std::env::var("DISABLE_O_DIRECT").is_err();

    // Graceful Shutdown Hook
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("\n[System]  Shutting down safely...");
        r.store(false, Ordering::SeqCst);
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    // Load Config globally for all commands
    let config: HuskConfig = if let Ok(contents) = std::fs::read_to_string(&cli.config) {
        toml::from_str(&contents).expect("Failed to parse config file")
    } else {
        println!("!!Config file not found. Generating default '{}'", cli.config);
        std::fs::write(&cli.config, DEFAULT_TOML).unwrap();
        toml::from_str(DEFAULT_TOML).unwrap()
    };
    let config_arc = Arc::new(config.clone());

    // Initialize logging based on TOML config
    let env = env_logger::Env::default().filter_or("RUST_LOG", &config_arc.log_level);
    env_logger::Builder::from_env(env).init();

    match &cli.command {
        Commands::Format { tape_dev } => {
            if let Err(e) = format_tape(tape_dev, use_direct_io) {
                error!("Format failed: {}", e);
            }
        }
        Commands::Daemon => {
            if unsafe { libc::geteuid() } != 0 {
                info!("!!Running Husk without root privileges.");
                info!("   If fanotify fails, grant capabilities to the binary using:");
                info!("   sudo setcap cap_sys_admin,cap_dac_read_search+ep ./target/release/husk");
            }

            std::fs::create_dir_all(&config_arc.hot_tier).unwrap();
            let startup_conn = init_catalog(&config_arc.db_path).expect("Failed to init catalog database");
            rescan_tape_drives(&startup_conn);
            drop(startup_conn); 
            
            info!("Husk Archiver Initialized (Grid & Policy Engine Mode).");
            info!("Hot Tier: ./{}", config_arc.hot_tier);
            info!("Primary Volumes: {}", config_arc.primary_volumes.join(", "));
            info!("Replication Targets: {}", config_arc.replication_volumes.join(", "));

            let (tx, rx) = mpsc::sync_channel(100);

            // 1. Spawn the Archive Worker (Handles the heavy lifting)
            let worker_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                run_archive_worker(rx, worker_config, use_direct_io);
            });

            // 2. Spawn the Janitor Policy Scanner (The Scheduler)
            let scanner_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                loop {
                    // --- SCHEDULER: Sleep BEFORE scanning ---
                    if let Some(ref schedule) = scanner_config.janitor_schedule_time {
                        if schedule.to_lowercase() != "none" && schedule.contains(':') {
                            let now = chrono::Local::now();
                            if let Ok(target_time) = chrono::NaiveTime::parse_from_str(schedule, "%H:%M") {
                                let mut target_dt = now.date_naive().and_time(target_time);
                                
                                // If the scheduled time has already passed today, wait for tomorrow's slot
                                if target_dt <= now.naive_local() {
                                    target_dt += chrono::Duration::days(1);
                                }
                                
                                let duration = target_dt - now.naive_local();
                                let secs_to_wait = duration.num_seconds().max(1) as u64;
                                
                                info!("[Janitor] Production Mode: Sleeping until {} ({} seconds remaining)...", schedule, secs_to_wait);
                                thread::sleep(std::time::Duration::from_secs(secs_to_wait));
                            } else {
                                error!("[Janitor]  Invalid schedule_time format '{}'. Use 'HH:MM'. Falling back to interval.", schedule);
                                thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                            }
                        } else {
                            // Test Mode: Just sleep for the interval
                            thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                        }
                    } else {
                        // Default Fallback
                        thread::sleep(std::time::Duration::from_secs(scanner_config.janitor_interval_secs));
                    }

                    // --- EXECUTION: Run the scan after the sleep period ends ---
                    info!("[Janitor] Starting scheduled policy scan...");
                    run_janitor_scanner(tx.clone(), Arc::clone(&scanner_config));
                }
            });

            // 3. Start HTTP Streaming Gateway (Persistent local media server bridge)
            let http_config = Arc::clone(&config_arc);
            thread::spawn(move || {
                run_http_gateway(http_config, use_direct_io);
            });

            // 4. Start Foreground Interceptor (Handles real-time OS restores)
            run_interceptor(config_arc, use_direct_io).unwrap();
        }
        Commands::Rebuild { tape_dev, output_db } => {
            rebuild_catalog(tape_dev, output_db, use_direct_io).unwrap();
        }
        Commands::Info { tape_dev } => {
            let target = tape_dev.as_deref().unwrap_or(&config_arc.primary_volumes[0]);
            print_tape_gauge(target, &config_arc.db_path);
        }
        Commands::Scrub { tape_dev } => {
            let target = tape_dev.as_deref().unwrap_or(&config_arc.primary_volumes[0]);
            if let Err(e) = scrub_tape(target, &config_arc.db_path, use_direct_io) {
                error!("Scrubber failed: {}", e);
            }
        }
        Commands::Restore { file_path, dest_path, version } => {
            if let Err(e) = manual_restore(&config_arc, &config_arc.db_path, file_path, dest_path, *version, use_direct_io) {
                error!("Manual restore failed: {}", e);
            }
        }
        
        Commands::Cat { file_path, offset, length, tape_uuid } => {
            log::set_max_level(log::LevelFilter::Debug);
            if let Err(e) = cat_file(&config_arc, &config_arc.db_path, file_path, *offset, *length, use_direct_io, tape_uuid.as_deref()) {
                eprintln!("Cat failed: {}", e);
            }
        }
        Commands::Repack { source_tape, dest_tape } => {
            if let Err(e) = repack_tape(&config_arc.db_path, source_tape, dest_tape, use_direct_io) {
                error!("Repacker failed: {}", e);
            }
        }
        Commands::Export { format, output } => {
            if format.to_lowercase() == "parquet" {
                info!("Exporting catalog to Parquet: {}", output);
                if let Err(e) = export_catalog_parquet(&config_arc.db_path, output) {
                    error!("Export failed: {}", e);
                } else {
                    info!("Successfully exported catalog to {}", output);
                    info!("Data Engineers can now query this using: duckdb -c \"SELECT * FROM '{}' LIMIT 10;\"", output);
                }
            } else {
                error!("Unsupported format: {}. Currently only 'parquet' is supported.", format);
            }
        }
    }
}
