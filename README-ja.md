> ⚠️ **注意:** このドキュメントはAIによって翻訳されたものです。技術的な用語や表現に不自然な点があるかもしれません。HuskHoardは日本のユーザーからの多大なサポートに感謝しています！翻訳の改善、修正、または日本語ドキュメントのメンテナーとして協力してくださる方のPull Requestを大歓迎しています。

# HuskHoard

![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Built with Rust](https://img.shields.io/badge/Built_with-Rust-orange.svg)
![platform-Linux](https://img.shields.io/badge/platform-Linux%20-red.svg)

**HuskHoard** はLinux向けのオープンソース・データ階層化アーカイブ（Data Tiering Archive）です。コールドデータを安価なハードドライブ（[SMR](https://huskhoard.com/blog-post-smr.html)や[CMR](https://huskhoard.com/blog-post-cmr.html)）、[LTOテープ](https://huskhoard.comom/blog-post-lto.html)、または[クラウドストレージ](https://huskhoard.com/blog-post-cloud.html)にバックグラウンドで自動的にアーカイブすることで、高価なNVMeドライブを「底なしのファイルシステム」に変えます。アーカイブされた後も、ファイルはOS上から完全に可視化され、いつでもアクセス可能な状態を保ちます。詳細は [www.huskhoard.com](http://www.huskhoard.com) をご覧ください。

モダンなハイブリッドクラウドユーザーのために構築された、エンタープライズ向けのテープライブラリのように機能します。
[LTO互換性リスト](https://huskhoard.com/compatibility.html) を確認し、今日からLTOアーカイブを構築しましょう！

## なぜ HuskHoard なのか？

エンタープライズ向けのストレージベンダーは、自動ストレージ階層化機能に数千ドルの費用を請求し、独自のブラックボックス内にデータを囲い込みます。HuskHoardは、標準的なオープンソースフォーマットを使用し、ユーザースペース（user-space）でこの機能を**無料**で実現します。オープンソースのデータ階層化アーカイブでデータを管理しましょう。

*   **ハードウェアの自由 (Bring Your Own Hardware):** あなたの「テープライブラリ」が1万ドルのSANであれ、**物理的なLTOテープドライブ**であれ、ホコリを被ったUSBドライブやAmazon S3バケットであれ、HuskHoardは柔軟に対応します。
*   **オーバーヘッドゼロの透過的スタブ化:** HuskHoardはFUSEを**使用しません**。LinuxカーネルAPIの `fanotify` を使用して、プロセスをリアルタイムにブロックおよび再開します。
*   **StreamGate HTTPゲートウェイ:** テープやS3からローカルHTTPブリッジを経由して4K動画を直接再生できます。これにより、Plex、Jellyfin、VLCなどのメディアサーバーが、SSDの容量を一切消費せずに巨大なファイルを瞬時にシークできます。
*   **簡単な移行保証（ベンダーロックインなし）:** ペイロードデータは、**BLAKE3**で検証された標準的な**Zstd**ストリームとして保存されます。カタログのメタデータは**Apache Parquet**形式でエクスポートでき、外部データベースで活用することが可能です。

#### 主な機能 (Features)
*   **StreamGate (インデックス化されたダイレクトアクセス):** ディスク消費ゼロの展開。ジャンプテーブルを使用して、テープやS3に保存された10TBのファイル全体をダウンロードすることなく、任意のバイトに瞬時にシークします。
*   **データエンジニアリング対応:** ファイルカタログ全体を**Apache Parquet**にエクスポート可能。DuckDB、Python、Sparkなどを使用して、大規模な監査、AIタグ付け、ストレージ分析を実行できます。
*   **ネイティブSCSIテープドライバー:** `/dev/nstX` を介したLTO-5からLTO-9ドライブのプロフェッショナルグレードサポート。ハードウェアのポジショニングや、「シューシャイニング（shoe-shining）」を防ぐための256KBブロックアライメントを処理します。
*   **N-Way レプリケーション:** ローカルドライブ、物理テープ、クラウドバケット（rclone経由）全体にコールドデータを同時に自動ミラーリングします。
*   **ハイウォーターマーク・スピルオーバー (容量超過時の自動退避):** SSDを自動的に保護します。ホットティア（Hot Tier）が設定された閾値（例: 80%）を超えると、HuskHoardは緊急アーカイブサイクルをトリガーします。

### アーキテクチャの概要 (Architecture Overview)
*   **The Catalog (カタログ):** すべてのファイル、バージョン履歴、および物理メディア上の正確なバイトオフセットを追跡するSQLiteの「頭脳」。
*   **The Interceptor (インターセプター):** アプリケーションがスタブ化された（実体のない）ファイルを要求したことを検知し、即座にリコール（復元）をトリガーする軽量なfanotifyループ。
*   **The Janitor (ジャニター/管理人):** 作成日、拡張子、またはディレクトリのルールに基づいてコールドデータを特定するバックグラウンドのポリシーエンジン。
*   **The Archive Worker (アーカイブワーカー):** 重い処理を担うコンポーネント。データをシーク可能なフレームに圧縮し、ストレージプールへの書き込みを多重化し、SCSIハードウェアコマンドを管理します。
*   **アーキテクチャの詳細:** ブログにはアーキテクチャに関する詳細な記事がいくつかあります。詳しくは www.huskhoard.com/blog.html をご覧ください。

### OSの互換性と要件 (OS Compatibility & Requirements)
HuskHoardはLinuxの **fanotify** カーネルAPIに依存しています。**カーネル5.1以上**を使用するモダンなLinuxディストリビューションと互換性があります。

*   **プライマリーサポート:** Ubuntu 22.04 LTS, 24.04 LTS (推奨)
*   **エンタープライズ/サーバー:** Debian 11/12, Rocky/AlmaLinux 8/9, RHEL 8/9
*   **ファイルシステム:** XFS, ZFS, Ext4, Btrfs
*   **非対応:** WSL2 (Windows), CentOS 7 (カーネルが古すぎるため), Synology/QNAP (カスタムカーネルを使用しない限り不可)。

### 🚀 クイックスタート (Ubuntu 24.04)
コンパイル済みのバイナリをインストールしたい場合は、こちらの [Quick Start](https://github.com/HuskHoard/HuskHoard/blob/main/Quick%20Start/Release.md) をご利用ください。

**⚠️ 重要:** すべてのコマンドは一般ユーザーとして実行してください。HuskHoardはユーザースペースで実行されるように設計されています。

#### 1. 前提条件 (Prerequisites)
```bash
sudo apt update
sudo apt install -y build-essential rclone libcap2-bin attr pkg-config libsqlite3-dev git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
