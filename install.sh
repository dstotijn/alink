#!/bin/sh
# Installs alink from its GitHub releases.
#
#   curl -fsSL https://dstotijn.github.io/alink/install.sh | sh
#
# Environment variables:
#   ALINK_VERSION      Release tag to install, for example v0.1.0 (default: latest).
#   ALINK_INSTALL_DIR  Directory for the binary (default: ~/.local/bin).
#   ALINK_DOWNLOAD_URL Base URL of the release files, for mirrors and testing.

set -eu

REPO="dstotijn/alink"

err() {
	echo "alink install: $*" >&2
	exit 1
}

detect_target() {
	os=$(uname -s)
	arch=$(uname -m)
	case "$os" in
	Darwin)
		# A shell under Rosetta reports x86_64 on Apple silicon; prefer the native build.
		if [ "$arch" = "x86_64" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
			arch="arm64"
		fi
		suffix="apple-darwin"
		;;
	Linux) suffix="unknown-linux-musl" ;;
	*) err "unsupported operating system: $os (alink supports macOS and Linux)" ;;
	esac
	case "$arch" in
	arm64 | aarch64) arch="aarch64" ;;
	x86_64 | amd64) arch="x86_64" ;;
	*) err "unsupported architecture: $arch" ;;
	esac
	echo "$arch-$suffix"
}

download() {
	if command -v curl >/dev/null 2>&1; then
		curl -fsSL --retry 3 -o "$2" "$1" || err "download failed: $1"
	elif command -v wget >/dev/null 2>&1; then
		wget -q -O "$2" "$1" || err "download failed: $1"
	else
		err "curl or wget is required"
	fi
}

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d ' ' -f 1
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | cut -d ' ' -f 1
	else
		err "sha256sum or shasum is required to verify the download"
	fi
}

main() {
	version="${ALINK_VERSION:-latest}"
	install_dir="${ALINK_INSTALL_DIR:-$HOME/.local/bin}"
	target=$(detect_target)
	if [ -n "${ALINK_DOWNLOAD_URL:-}" ]; then
		base="$ALINK_DOWNLOAD_URL"
	elif [ "$version" = "latest" ]; then
		base="https://github.com/$REPO/releases/latest/download"
	else
		base="https://github.com/$REPO/releases/download/$version"
	fi
	archive="alink-$target.tar.gz"

	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT INT TERM

	echo "Downloading alink ($version, $target)..."
	download "$base/$archive" "$tmp/$archive"
	download "$base/$archive.sha256" "$tmp/$archive.sha256"

	expected=$(cut -d ' ' -f 1 <"$tmp/$archive.sha256")
	actual=$(sha256 "$tmp/$archive")
	[ "$expected" = "$actual" ] || err "checksum mismatch for $archive (expected $expected, got $actual)"

	tar -xzf "$tmp/$archive" -C "$tmp"
	mkdir -p "$install_dir"
	# Copy next to the destination, then rename, so a running alink is replaced atomically.
	cp "$tmp/alink-$target/alink" "$install_dir/.alink.tmp"
	chmod 755 "$install_dir/.alink.tmp"
	mv "$install_dir/.alink.tmp" "$install_dir/alink"

	echo "Installed $("$install_dir/alink" --version) to $install_dir/alink"
	case ":$PATH:" in
	*":$install_dir:"*) ;;
	*) echo "Add $install_dir to your PATH to run alink." ;;
	esac
	echo "Next, install the agent skill: npx skills add $REPO"
}

main "$@"
