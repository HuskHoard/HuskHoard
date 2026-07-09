
## Quick Start (Pre-compiled Binary)

If you prefer not to build HuskHoard from source, you can use our pre-compiled release binaries. 

**1. Download the Latest Release**
Fetch the latest binary from the GitHub Releases page. (Assuming a standard Linux `x86_64` environment):

```bash
# Download the latest release archive (Check the Releases page for the exact filename)
wget https://github.com/huskhoard/huskhoard/releases/latest/download/huskhoard-linux-amd64.tar.gz

# Extract the archive
tar -xzf huskhoard-linux-amd64.tar.gz
cd huskhoard-linux-amd64
```

**2. Make the Binary Executable**
```bash
chmod +x huskhoard
```

**3. Grant Kernel Capabilities**
Because HuskHoard uses the Linux `fanotify` API to transparently intercept file reads without needing to run entirely as a dangerous root process, you must grant the executable specific capabilities:

```bash
sudo setcap cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep ./huskhoard
```
*(Note: If your system uses different capabilities, refer to the advanced documentation).*

**4. Move to your PATH (Optional but Recommended)**
If you want HuskHoard accessible system-wide, move it to `/usr/local/bin`. 
**Important:** Moving the binary strips its capabilities, so you must re-apply them after moving!

```bash
sudo mv ./huskhoard /usr/local/bin/huskhoard
sudo setcap cap_sys_admin,cap_sys_ptrace,cap_dac_read_search+ep /usr/local/bin/huskhoard
```

**5. Verify Installation**
Check that HuskHoard is installed and running properly:
```bash
huskhoard --help
```

You are now ready to configure your storage tiers and start the archiving engine. 
```
