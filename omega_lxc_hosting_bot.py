#!/usr/bin/env python3
"""
OMEGA LXC HOSTING BOT
=====================

Discord control plane for an Ubuntu VM running LXD/LXC.

Core features
-------------
• Local node is automatically the default node.
• Add/remove external LXD nodes over SSH.
• Owner-controlled admin system.
• Admin-only VPS deployment.
• Beautiful /manage and !manage control panels.
• Embedded command output.
• Start / stop / restart / force-stop.
• OS reinstall:
    Ubuntu 20.04 / 22.04 / 24.04
    Debian 11 / 12
    Kali Linux
    Fedora
• SSH access using tmate OR sshx.
• Resource limits: CPU, RAM, disk.
• Snapshots and snapshot restore.
• Container clone.
• Container console / exec.
• Container IP + network information.
• LXD config inspection.
• Audit logging.
• Per-admin/user container ownership.
• Node health monitoring.
• Host statistics.
• Optional user quotas.
• Persistent SQLite database.
• No shell=True for host commands.

INSTALL
-------
    sudo apt update
    sudo apt install -y python3 python3-venv lxd openssh-client

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

Environment:
    DISCORD_TOKEN=...
    OWNER_ID=1127965708190486570
    DB_PATH=omega_lxc.db
    PREFIX=!

The bot account needs:
    applications.commands
    bot permissions appropriate for your Discord server.

The Ubuntu VM needs permission to use:
    lxc list
    lxc launch
    lxc exec
    lxc config
    lxc snapshot
    lxc copy
    lxc delete

IMPORTANT SECURITY NOTES
------------------------
1. The Discord bot is a privileged infrastructure controller.
2. Run it under a dedicated service account with only the LXD permissions
   you actually need.
3. Do NOT expose the LXD UNIX socket directly to the Internet.
4. External nodes should use SSH keys and a restricted automation account.
5. /exec is intentionally restricted to container owners/admins.
6. tmate/sshx sessions are created INSIDE the selected container.
"""

import asyncio
import base64
import os
import re
import shlex
import time
from dataclasses import dataclass
from typing import Optional

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks


# ============================================================
# CONFIG
# ============================================================

TOKEN = os.getenv("DISCORD_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "1127965708190486570"))
DB_PATH = os.getenv("DB_PATH", "omega_lxc.db")
PREFIX = os.getenv("PREFIX", "!")

MAX_OUTPUT = 3800
COMMAND_TIMEOUT = 45
EXEC_TIMEOUT = 35
MAX_CONTAINERS_PER_OWNER = 15

# Change these to the exact image aliases available on your LXD server.
OS_IMAGES = {
    "Ubuntu 20": "images:ubuntu/20.04",
    "Ubuntu 22": "images:ubuntu/22.04",
    "Ubuntu 24": "images:ubuntu/24.04",
    "Debian 11": "images:debian/11",
    "Debian 12": "images:debian/12",
    "Kali Linux": "images:kali/current",
    "Fedora": "images:fedora/42",
}

OS_ALIASES = {
    "ubuntu20": "Ubuntu 20",
    "ubuntu22": "Ubuntu 22",
    "ubuntu24": "Ubuntu 24",
    "debian11": "Debian 11",
    "debian12": "Debian 12",
    "kali": "Kali Linux",
    "fedora": "Fedora",
}


# ============================================================
# EMBEDS
# ============================================================

def make_embed(
    title: str,
    description: str = "",
    color: discord.Color = discord.Color.blurple(),
) -> discord.Embed:
    e = discord.Embed(
        title=title,
        description=description[:4096],
        color=color,
        timestamp=discord.utils.utcnow(),
    )
    return e


def truncate(text: str, limit: int = MAX_OUTPUT) -> str:
    text = (text or "").strip()
    if not text:
        return "(no output)"
    if len(text) <= limit:
        return text
    return text[:limit] + "\n… output truncated"


def codebox(text: str, limit: int = MAX_OUTPUT) -> str:
    return f"```text\n{truncate(text, limit)}\n```"


def valid_name(name: str) -> bool:
    return bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,31}", name))


# ============================================================
# PROCESS ENGINE
# ============================================================

@dataclass
class Result:
    ok: bool
    output: str
    returncode: int = 0


async def run_argv(
    argv: list[str],
    timeout: int = COMMAND_TIMEOUT,
    stdin: Optional[str] = None,
) -> Result:
    """Run a program without shell=True."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE if stdin is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(stdin.encode() if stdin is not None else None),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return Result(False, "Command timed out.", -1)

        out = stdout.decode(errors="replace")
        err = stderr.decode(errors="replace")
        text = out if out.strip() else err

        return Result(proc.returncode == 0, text, proc.returncode)

    except FileNotFoundError:
        return Result(False, f"Executable not found: {argv[0]}", -1)
    except Exception as exc:
        return Result(False, f"{type(exc).__name__}: {exc}", -1)


async def run_node(node_id: int, argv: list[str], timeout=COMMAND_TIMEOUT) -> Result:
    row = await db_one(
        "SELECT ip, ssh_user, is_local FROM nodes WHERE id=?",
        (node_id,),
    )

    if not row:
        return Result(False, "Node does not exist.", -1)

    ip, ssh_user, is_local = row

    if is_local:
        return await run_argv(argv, timeout)

    # SSH arguments remain separated; user input is not inserted into a
    # local shell command.
    ssh = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new",
        f"{ssh_user}@{ip}",
        "--",
        *argv,
    ]
    return await run_argv(ssh, timeout)


async def lxc(node_id: int, *args: str, timeout=COMMAND_TIMEOUT) -> Result:
    return await run_node(node_id, ["lxc", *args], timeout)


# ============================================================
# DATABASE
# ============================================================

async def db_one(query: str, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cur:
            return await cur.fetchone()


async def db_all(query: str, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(query, params) as cur:
            return await cur.fetchall()


async def db_exec(query: str, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            ip TEXT NOT NULL,
            ssh_user TEXT NOT NULL DEFAULT 'root',
            is_local INTEGER NOT NULL DEFAULT 0,
            online INTEGER NOT NULL DEFAULT 0,
            last_check INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS containers (
            name TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            node_id INTEGER NOT NULL,
            os TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            last_action TEXT NOT NULL DEFAULT 'created'
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            target TEXT,
            details TEXT,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        INSERT OR IGNORE INTO nodes
            (id, name, ip, ssh_user, is_local, online)
        VALUES
            (1, 'Local Node', '127.0.0.1', 'root', 1, 0);
        """)
        await db.commit()


async def audit(user_id: int, action: str, target="", details=""):
    await db_exec(
        """INSERT INTO audit_log
           (user_id, action, target, details, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (user_id, action, target, details[:1500], int(time.time())),
    )


# ============================================================
# AUTHORIZATION
# ============================================================

async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True

    return (
        await db_one(
            "SELECT 1 FROM admins WHERE user_id=?",
            (user_id,),
        )
    ) is not None


async def owns_container(user_id: int, name: str) -> bool:
    return (
        await db_one(
            "SELECT 1 FROM containers WHERE name=? AND owner_id=?",
            (name, user_id),
        )
    ) is not None


async def container_row(name: str):
    return await db_one(
        """SELECT name, owner_id, node_id, os
           FROM containers WHERE name=?""",
        (name,),
    )


async def can_manage(user_id: int, name: str) -> bool:
    if await is_admin(user_id):
        return True
    return await owns_container(user_id, name)


def admin_check():
    async def predicate(interaction: discord.Interaction):
        if await is_admin(interaction.user.id):
            return True

        if not interaction.response.is_done():
            await interaction.response.send_message(
                "🛡️ **Admin access required.**",
                ephemeral=True,
            )
        return False

    return app_commands.check(predicate)


# ============================================================
# LXC INFORMATION
# ============================================================

async def container_status(node_id: int, name: str):
    r = await lxc(node_id, "info", name, timeout=15)

    if not r.ok:
        return "UNKNOWN", r.output

    if "Status: RUNNING" in r.output:
        return "RUNNING", r.output

    if "Status: STOPPED" in r.output:
        return "STOPPED", r.output

    return "UNKNOWN", r.output


async def container_ip(node_id: int, name: str):
    r = await lxc(
        node_id,
        "list",
        name,
        "--format",
        "csv",
        "-c",
        "4",
        timeout=15,
    )

    if not r.ok:
        return "N/A"

    ips = []

    for item in r.output.split(","):
        item = item.strip()
        if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", item):
            ips.append(item)

    return ", ".join(dict.fromkeys(ips)) or "N/A"


async def container_addresses(node_id: int, name: str):
    r = await lxc(
        node_id,
        "exec",
        name,
        "--",
        "sh",
        "-lc",
        "ip -brief addr 2>/dev/null || ip addr",
        timeout=15,
    )
    return r.output


def status_badge(status: str):
    return {
        "RUNNING": "🟢 RUNNING",
        "STOPPED": "🔴 STOPPED",
        "UNKNOWN": "🟡 UNKNOWN",
    }.get(status, status)


# ============================================================
# BOT
# ============================================================

class OmegaBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(
            command_prefix=PREFIX,
            intents=intents,
            help_command=None,
        )

        self.started = time.monotonic()

    async def setup_hook(self):
        await init_db()

        # Global slash commands.
        await self.tree.sync()

        self.health_loop.start()

    async def on_ready(self):
        print("╔══════════════════════════════════════════════╗")
        print("║              OMEGA LXC HOSTING              ║")
        print("║           Discord Infrastructure             ║")
        print("╚══════════════════════════════════════════════╝")
        print(f"Logged in as: {self.user} ({self.user.id})")

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="LXC infrastructure",
            )
        )

    @tasks.loop(minutes=5)
    async def health_loop(self):
        try:
            rows = await db_all("SELECT id FROM nodes")

            for (node_id,) in rows:
                result = await lxc(node_id, "version", timeout=10)

                await db_exec(
                    """UPDATE nodes
                       SET online=?, last_check=?
                       WHERE id=?""",
                    (1 if result.ok else 0, int(time.time()), node_id),
                )
        except Exception as exc:
            print("[health]", exc)

    @health_loop.before_loop
    async def before_health(self):
        await self.wait_until_ready()


bot = OmegaBot()


# ============================================================
# CONFIRMATION VIEW
# ============================================================

class ConfirmView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=30)
        self.user_id = user_id
        self.confirmed = False

    async def interaction_check(self, interaction):
        if interaction.user.id != self.user_id and not await is_admin(interaction.user.id):
            await interaction.response.send_message(
                "❌ This confirmation belongs to another user.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Confirm",
        emoji="✅",
        style=discord.ButtonStyle.danger,
    )
    async def yes(self, interaction, button):
        self.confirmed = True

        for child in self.children:
            child.disabled = True

        await interaction.response.edit_message(
            content="✅ Confirmed. Processing...",
            view=self,
        )
        self.stop()

    @discord.ui.button(
        label="Cancel",
        emoji="✖️",
        style=discord.ButtonStyle.secondary,
    )
    async def no(self, interaction, button):
        self.confirmed = False

        for child in self.children:
            child.disabled = True

        await interaction.response.edit_message(
            content="❌ Cancelled.",
            view=self,
        )
        self.stop()


# ============================================================
# SSH ACCESS
# ============================================================

async def ensure_network_tool(
    node_id: int,
    container: str,
    tool: str,
) -> Result:
    """
    Install tmate or sshx inside the container.

    This function detects apt/apk. Fedora gets dnf.
    """
    detect = await lxc(
        node_id,
        "exec",
        container,
        "--",
        "sh",
        "-lc",
        "command -v apt-get >/dev/null && echo apt; "
        "command -v apk >/dev/null && echo apk; "
        "command -v dnf >/dev/null && echo dnf",
        timeout=10,
    )

    if not detect.ok:
        return detect

    manager = detect.output.strip().splitlines()[0] if detect.output.strip() else ""

    if tool == "tmate":
        if manager == "apt":
            return await lxc(
                node_id,
                "exec",
                container,
                "--",
                "sh",
                "-lc",
                "apt-get update -qq && apt-get install -y tmate",
                timeout=120,
            )

        if manager == "apk":
            return await lxc(
                node_id,
                "exec",
                container,
                "--",
                "sh",
                "-lc",
                "apk add --no-cache tmate",
                timeout=120,
            )

        if manager == "dnf":
            return await lxc(
                node_id,
                "exec",
                container,
                "--",
                "sh",
                "-lc",
                "dnf install -y tmate",
                timeout=120,
            )

    if tool == "sshx":
        # Official installer is fetched by curl inside the container.
        # It is only executed after the user explicitly selects SSHX.
        return await lxc(
            node_id,
            "exec",
            container,
            "--",
            "sh",
            "-lc",
            "command -v curl >/dev/null || "
            "(command -v apt-get >/dev/null && "
            "apt-get update -qq && apt-get install -y curl) || "
            "(command -v apk >/dev/null && apk add --no-cache curl)",
            timeout=120,
        )

    return Result(False, "Unsupported access method.", -1)


async def get_tmate_session(node_id: int, container: str) -> Result:
    """
    Starts tmate and extracts the SSH connection URL.

    tmate needs outbound Internet access from the container.
    """
    installed = await ensure_network_tool(node_id, container, "tmate")
    if not installed.ok:
        return installed

    command = (
        "tmate -S /tmp/omega-tmate new-session -d; "
        "tmate -S /tmp/omega-tmate wait tmate-ready; "
        "tmate -S /tmp/omega-tmate display -p '#{tmate_ssh}'"
    )

    return await lxc(
        node_id,
        "exec",
        container,
        "--",
        "sh",
        "-lc",
        command,
        timeout=45,
    )


async def get_sshx_session(node_id: int, container: str) -> Result:
    """
    Starts sshx and returns its terminal output.

    sshx changes its installer/CLI occasionally, so the command is kept
    isolated here for easy updating.
    """
    installed = await ensure_network_tool(node_id, container, "sshx")
    if not installed.ok:
        return installed

    command = (
        "command -v sshx >/dev/null || "
        "curl -sSf https://sshx.io/get | sh; "
        "sshx"
    )

    return await lxc(
        node_id,
        "exec",
        container,
        "--",
        "sh",
        "-lc",
        command,
        timeout=45,
    )


# ============================================================
# CONTROL PANEL
# ============================================================

class ManageView(discord.ui.View):
    def __init__(self, name: str, node_id: int, owner_id: int):
        super().__init__(timeout=1800)
        self.name = name
        self.node_id = node_id
        self.owner_id = owner_id

    async def interaction_check(self, interaction):
        if await can_manage(interaction.user.id, self.name):
            return True

        await interaction.response.send_message(
            "❌ You do not have permission to manage this container.",
            ephemeral=True,
        )
        return False

    async def refresh_message(self, interaction):
        status, info = await container_status(self.node_id, self.name)
        ip = await container_ip(self.node_id, self.name)

        row = await container_row(self.name)
        os_name = row[3] if row else "Unknown"

        e = make_embed(
            f"🎛️ {self.name}",
            "## Omega LXC Control Center\n"
            "Manage your VPS using the controls below.",
            discord.Color.green()
            if status == "RUNNING"
            else discord.Color.red(),
        )

        e.add_field(name="Status", value=status_badge(status), inline=True)
        e.add_field(name="IPv4", value=f"`{ip}`", inline=True)
        e.add_field(name="OS", value=f"`{os_name}`", inline=True)
        e.add_field(name="Node", value=f"`{self.node_id}`", inline=True)
        e.add_field(
            name="Quick information",
            value=codebox(info, 1300),
            inline=False,
        )
        e.set_footer(text="Omega Hosting • LXC Control Center")

        await interaction.response.edit_message(
            embed=e,
            view=self,
        )

    @discord.ui.button(
        label="Start",
        emoji="▶️",
        style=discord.ButtonStyle.success,
        row=0,
    )
    async def start(self, interaction, button):
        result = await lxc(self.node_id, "start", self.name)
        await audit(interaction.user.id, "start", self.name, result.output)

        await interaction.response.send_message(
            embed=make_embed(
                "▶️ Start",
                "Container started." if result.ok else codebox(result.output),
                discord.Color.green() if result.ok else discord.Color.red(),
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Stop",
        emoji="⏹️",
        style=discord.ButtonStyle.danger,
        row=0,
    )
    async def stop(self, interaction, button):
        result = await lxc(self.node_id, "stop", self.name, "--force")
        await audit(interaction.user.id, "stop", self.name, result.output)

        await interaction.response.send_message(
            embed=make_embed(
                "⏹️ Stop",
                "Container stopped." if result.ok else codebox(result.output),
                discord.Color.green() if result.ok else discord.Color.red(),
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Restart",
        emoji="🔄",
        style=discord.ButtonStyle.primary,
        row=0,
    )
    async def restart(self, interaction, button):
        result = await lxc(self.node_id, "restart", self.name, "--force")
        await audit(interaction.user.id, "restart", self.name, result.output)

        await interaction.response.send_message(
            embed=make_embed(
                "🔄 Restart",
                "Container restarted." if result.ok else codebox(result.output),
                discord.Color.green() if result.ok else discord.Color.red(),
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Refresh",
        emoji="🔃",
        style=discord.ButtonStyle.secondary,
        row=0,
    )
    async def refresh(self, interaction, button):
        await self.refresh_message(interaction)

    @discord.ui.button(
        label="SSH Access",
        emoji="🔐",
        style=discord.ButtonStyle.blurple,
        row=1,
    )
    async def ssh_access(self, interaction, button):
        view = SSHView(self.name, self.node_id, self.owner_id)

        e = make_embed(
            "🔐 SSH Access",
            "Choose a temporary browser/terminal access method.\n\n"
            "• **tmate** — creates a temporary SSH session\n"
            "• **sshx** — creates a web terminal session",
        )

        await interaction.response.send_message(
            embed=e,
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="IP / Network",
        emoji="🌐",
        style=discord.ButtonStyle.secondary,
        row=1,
    )
    async def network(self, interaction, button):
        ip = await container_ip(self.node_id, self.name)
        addresses = await container_addresses(self.node_id, self.name)

        e = make_embed("🌐 Network Information")
        e.add_field(name="Detected IPv4", value=f"`{ip}`", inline=False)
        e.add_field(name="Interfaces", value=codebox(addresses, 2200), inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)

    @discord.ui.button(
        label="Stats",
        emoji="📊",
        style=discord.ButtonStyle.secondary,
        row=1,
    )
    async def stats(self, interaction, button):
        result = await lxc(
            self.node_id,
            "info",
            self.name,
            "--resources",
        )

        await interaction.response.send_message(
            embed=make_embed(
                "📊 Container Resources",
                codebox(result.output),
                discord.Color.blurple(),
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Snapshots",
        emoji="💾",
        style=discord.ButtonStyle.secondary,
        row=2,
    )
    async def snapshots(self, interaction, button):
        await interaction.response.send_message(
            embed=make_embed(
                "💾 Snapshots",
                "Use `/snapshot`, `/snapshots`, or `/restore` for snapshot management.",
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Reinstall OS",
        emoji="♻️",
        style=discord.ButtonStyle.danger,
        row=2,
    )
    async def reinstall(self, interaction, button):
        await interaction.response.send_message(
            embed=make_embed(
                "♻️ Reinstall OS",
                "Select the OS you want to install.",
                discord.Color.orange(),
            ),
            view=OSReinstallView(
                self.name,
                self.node_id,
                self.owner_id,
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Delete",
        emoji="🗑️",
        style=discord.ButtonStyle.danger,
        row=2,
    )
    async def delete(self, interaction, button):
        view = ConfirmView(interaction.user.id)

        await interaction.response.send_message(
            f"⚠️ **PERMANENT ACTION**\n\n"
            f"Delete `{self.name}` and all its data?",
            view=view,
            ephemeral=True,
        )

        await view.wait()

        if not view.confirmed:
            return

        result = await lxc(
            self.node_id,
            "delete",
            self.name,
            "--force",
            timeout=60,
        )

        if result.ok:
            await db_exec(
                "DELETE FROM containers WHERE name=?",
                (self.name,),
            )
            await audit(interaction.user.id, "delete", self.name)

            await interaction.followup.send(
                embed=make_embed(
                    "🗑️ Container deleted",
                    f"`{self.name}` has been deleted.",
                    discord.Color.red(),
                ),
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                embed=make_embed(
                    "❌ Delete failed",
                    codebox(result.output),
                    discord.Color.red(),
                ),
                ephemeral=True,
            )


class SSHView(discord.ui.View):
    def __init__(self, name: str, node_id: int, owner_id: int):
        super().__init__(timeout=180)
        self.name = name
        self.node_id = node_id
        self.owner_id = owner_id

    async def interaction_check(self, interaction):
        if await can_manage(interaction.user.id, self.name):
            return True

        await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )
        return False

    @discord.ui.button(
        label="Get tmate SSH",
        emoji="🔑",
        style=discord.ButtonStyle.primary,
    )
    async def tmate(self, interaction, button):
        await interaction.response.defer(ephemeral=True)

        result = await get_tmate_session(
            self.node_id,
            self.name,
        )

        await audit(
            interaction.user.id,
            "tmate",
            self.name,
        )

        if result.ok:
            text = result.output.strip()

            e = make_embed(
                "🔑 tmate SSH Session",
                "Your temporary SSH access is ready.\n"
                "Treat this URL as a secret.",
                discord.Color.green(),
            )
            e.add_field(
                name="SSH command",
                value=codebox(text, 1500),
                inline=False,
            )
        else:
            e = make_embed(
                "❌ tmate failed",
                codebox(result.output),
                discord.Color.red(),
            )

        await interaction.followup.send(
            embed=e,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Get sshx",
        emoji="🌐",
        style=discord.ButtonStyle.success,
    )
    async def sshx(self, interaction, button):
        await interaction.response.defer(ephemeral=True)

        result = await get_sshx_session(
            self.node_id,
            self.name,
        )

        await audit(
            interaction.user.id,
            "sshx",
            self.name,
        )

        e = make_embed(
            "🌐 sshx Session",
            "sshx output from the container:",
            discord.Color.green() if result.ok else discord.Color.red(),
        )
        e.add_field(
            name="Session",
            value=codebox(result.output, 2800),
            inline=False,
        )

        await interaction.followup.send(
            embed=e,
            ephemeral=True,
        )


class OSSelect(discord.ui.Select):
    def __init__(self, parent_view):
        options = [
            discord.SelectOption(
                label=name,
                value=name,
                emoji="🐧" if "Ubuntu" in name else
                      "🌀" if "Debian" in name else
                      "💀" if "Kali" in name else
                      "🎩",
            )
            for name in OS_IMAGES
        ]

        super().__init__(
            placeholder="Select an operating system...",
            min_values=1,
            max_values=1,
            options=options,
        )

        self.parent_view = parent_view

    async def callback(self, interaction):
        selected = self.values[0]

        confirm = ConfirmView(interaction.user.id)

        await interaction.response.send_message(
            f"⚠️ Reinstall `{self.parent_view.name}` with **{selected}**?\n\n"
            "All current container data will be destroyed.",
            view=confirm,
            ephemeral=True,
        )

        await confirm.wait()

        if not confirm.confirmed:
            return

        await interaction.followup.send(
            f"♻️ Reinstalling `{self.parent_view.name}`...",
            ephemeral=True,
        )

        await reinstall_container(
            interaction,
            self.parent_view.name,
            self.parent_view.node_id,
            selected,
        )


class OSReinstallView(discord.ui.View):
    def __init__(self, name, node_id, owner_id):
        super().__init__(timeout=120)
        self.name = name
        self.node_id = node_id
        self.owner_id = owner_id
        self.add_item(OSSelect(self))


# ============================================================
# REINSTALL
# ============================================================

async def reinstall_container(
    interaction,
    name: str,
    node_id: int,
    os_name: str,
):
    old = await lxc(node_id, "stop", name, "--force", timeout=30)

    if not old.ok and "not running" not in old.output.lower():
        return await interaction.followup.send(
            embed=make_embed(
                "❌ Reinstall failed",
                codebox(old.output),
                discord.Color.red(),
            ),
            ephemeral=True,
        )

    deleted = await lxc(
        node_id,
        "delete",
        name,
        "--force",
        timeout=60,
    )

    if not deleted.ok:
        return await interaction.followup.send(
            embed=make_embed(
                "❌ Could not delete old container",
                codebox(deleted.output),
                discord.Color.red(),
            ),
            ephemeral=True,
        )

    launched = await lxc(
        node_id,
        "launch",
        OS_IMAGES[os_name],
        name,
        timeout=120,
    )

    if not launched.ok:
        return await interaction.followup.send(
            embed=make_embed(
                "❌ New OS failed to launch",
                codebox(launched.output),
                discord.Color.red(),
            ),
            ephemeral=True,
        )

    await db_exec(
        """UPDATE containers
           SET os=?, last_action=?
           WHERE name=?""",
        (os_name, "reinstalled"),
    )

    await audit(
        interaction.user.id,
        "reinstall",
        name,
        os_name,
    )

    await interaction.followup.send(
        embed=make_embed(
            "♻️ OS Reinstalled",
            f"`{name}` is now running **{os_name}**.",
            discord.Color.green(),
        ),
        ephemeral=True,
    )


# ============================================================
# SLASH COMMANDS
# ============================================================

@bot.tree.command(
    name="help",
    description="Show the Omega Hosting command center",
)
async def help_command(interaction):
    e = make_embed(
        "⚡ OMEGA HOSTING",
        "Discord-powered LXC hosting control plane.",
    )

    e.add_field(
        name="🚀 Deployment",
        value="`/create` `/list` `/manage`",
        inline=False,
    )

    e.add_field(
        name="🎛️ VPS Management",
        value=(
            "`/info` `/exec` `/network` `/stats` "
            "`/limits` `/reinstall`"
        ),
        inline=False,
    )

    e.add_field(
        name="💾 Backups",
        value="`/snapshot` `/snapshots` `/restore` `/clone`",
        inline=False,
    )

    e.add_field(
        name="🌐 Infrastructure",
        value="`/nodes` `/nodeinfo` `/addnode` `/removenode`",
        inline=False,
    )

    e.add_field(
        name="🛡️ Administration",
        value="`/addadmin` `/removeadmin` `/admins` `/audit`",
        inline=False,
    )

    e.set_footer(text="Omega LXC Hosting • Owner controlled")

    await interaction.response.send_message(
        embed=e,
        ephemeral=True,
    )


@bot.tree.command(
    name="create",
    description="Deploy an LXC VPS",
)
@app_commands.describe(
    name="VPS/container name",
    os="Operating system",
    node="Node ID (default: 1)",
)
async def create(
    interaction,
    name: str,
    os: str,
    node: int = 1,
):
    if not await is_admin(interaction.user.id):
        return await interaction.response.send_message(
            "🛡️ Only the owner-approved admins can deploy VPS containers.",
            ephemeral=True,
        )

    if not valid_name(name):
        return await interaction.response.send_message(
            "❌ Invalid container name.",
            ephemeral=True,
        )

    if os not in OS_IMAGES:
        return await interaction.response.send_message(
            "❌ Invalid OS. Use `/help` or `/create` autocomplete.",
            ephemeral=True,
        )

    if await db_one(
        "SELECT 1 FROM containers WHERE name=?",
        (name,),
    ):
        return await interaction.response.send_message(
            "❌ A container with that name is already registered.",
            ephemeral=True,
        )

    node_row = await db_one(
        "SELECT id,name FROM nodes WHERE id=?",
        (node,),
    )

    if not node_row:
        return await interaction.response.send_message(
            "❌ Node not found.",
            ephemeral=True,
        )

    count = await db_one(
        "SELECT COUNT(*) FROM containers WHERE owner_id=?",
        (interaction.user.id,),
    )

    if not await is_admin(interaction.user.id) and count[0] >= MAX_CONTAINERS_PER_OWNER:
        return await interaction.response.send_message(
            "❌ Container quota reached.",
            ephemeral=True,
        )

    await interaction.response.defer()

    result = await lxc(
        node,
        "launch",
        OS_IMAGES[os],
        name,
        timeout=120,
    )

    if not result.ok:
        return await interaction.followup.send(
            embed=make_embed(
                "❌ Deployment failed",
                codebox(result.output),
                discord.Color.red(),
            ),
        )

    await db_exec(
        """INSERT INTO containers
           (name, owner_id, node_id, os, created_at, last_action)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            name,
            interaction.user.id,
            node,
            os,
            int(time.time()),
            "created",
        ),
    )

    await audit(
        interaction.user.id,
        "create",
        name,
        f"os={os};node={node}",
    )

    ip = await container_ip(node, name)

    e = make_embed(
        "🚀 VPS DEPLOYED",
        f"Your LXC VPS **{name}** is ready.",
        discord.Color.green(),
    )
    e.add_field(name="Operating System", value=f"`{os}`", inline=True)
    e.add_field(name="Node", value=f"`{node_row[1]}`", inline=True)
    e.add_field(name="IPv4", value=f"`{ip}`", inline=True)
    e.add_field(
        name="Manage",
        value=f"Use `/manage name:{name}` or `!manage {name}`.",
        inline=False,
    )

    await interaction.followup.send(embed=e)


@bot.tree.command(
    name="list",
    description="List registered VPS containers",
)
async def list_command(interaction):
    if await is_admin(interaction.user.id):
        rows = await db_all(
            """SELECT name, owner_id, node_id, os, created_at
               FROM containers
               ORDER BY created_at DESC"""
        )
    else:
        rows = await db_all(
            """SELECT name, owner_id, node_id, os, created_at
               FROM containers
               WHERE owner_id=?
               ORDER BY created_at DESC""",
            (interaction.user.id,),
        )

    if not rows:
        return await interaction.response.send_message(
            embed=make_embed(
                "📦 VPS List",
                "No containers registered.",
            ),
            ephemeral=True,
        )

    lines = []

    for name, owner_id, node_id, os_name, created in rows[:40]:
        status, _ = await container_status(node_id, name)

        owner = f" • <@{owner_id}>" if await is_admin(interaction.user.id) else ""

        lines.append(
            f"{status_badge(status)} **{name}**\n"
            f"└ `{os_name}` • node `{node_id}`{owner} • <t:{created}:R>"
        )

    await interaction.response.send_message(
        embed=make_embed(
            "📦 VPS Fleet",
            "\n\n".join(lines),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="manage",
    description="Open the embedded LXC management panel",
)
async def manage(interaction, name: str):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Container not found or access denied.",
            ephemeral=True,
        )

    row = await container_row(name)

    cname, owner_id, node_id, os_name = row

    status, info = await container_status(node_id, name)
    ip = await container_ip(node_id, name)

    e = make_embed(
        f"🎛️ {cname}",
        "## Omega LXC Control Center\n"
        "Everything important for this VPS is available below.",
        discord.Color.green()
        if status == "RUNNING"
        else discord.Color.red(),
    )

    e.add_field(name="Status", value=status_badge(status), inline=True)
    e.add_field(name="IPv4", value=f"`{ip}`", inline=True)
    e.add_field(name="OS", value=f"`{os_name}`", inline=True)
    e.add_field(name="Node", value=f"`{node_id}`", inline=True)
    e.add_field(name="Owner", value=f"<@{owner_id}>", inline=True)

    e.add_field(
        name="LXC Information",
        value=codebox(info, 1500),
        inline=False,
    )

    e.set_footer(text="Omega Hosting • Secure LXC Control")

    await interaction.response.send_message(
        embed=e,
        view=ManageView(
            name,
            node_id,
            owner_id,
        ),
    )


@bot.tree.command(
    name="exec",
    description="Run a command inside a VPS",
)
async def exec_command(
    interaction,
    name: str,
    command: str,
):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    if not command.strip():
        return await interaction.response.send_message(
            "❌ Command cannot be empty.",
            ephemeral=True,
        )

    row = await container_row(name)
    node_id = row[2]

    result = await lxc(
        node_id,
        "exec",
        name,
        "--",
        "sh",
        "-lc",
        command,
        timeout=EXEC_TIMEOUT,
    )

    await audit(
        interaction.user.id,
        "exec",
        name,
        command,
    )

    e = make_embed(
        f"💻 Command Output • {name}",
        codebox(result.output),
        discord.Color.green()
        if result.ok
        else discord.Color.red(),
    )
    e.add_field(
        name="Exit code",
        value=f"`{result.returncode}`",
        inline=True,
    )

    await interaction.response.send_message(
        embed=e,
        ephemeral=True,
    )


@bot.tree.command(
    name="info",
    description="Show complete LXC information",
)
async def info(interaction, name: str):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    row = await container_row(name)

    result = await lxc(
        row[2],
        "info",
        name,
        "--resources",
    )

    await interaction.response.send_message(
        embed=make_embed(
            f"📋 {name}",
            codebox(result.output),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="network",
    description="Show container network interfaces",
)
async def network(interaction, name: str):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    row = await container_row(name)
    ip = await container_ip(row[2], name)
    addresses = await container_addresses(row[2], name)

    e = make_embed("🌐 Network")
    e.add_field(name="IPv4", value=f"`{ip}`", inline=False)
    e.add_field(name="Interfaces", value=codebox(addresses), inline=False)

    await interaction.response.send_message(
        embed=e,
        ephemeral=True,
    )


@bot.tree.command(
    name="stats",
    description="Show container resource information",
)
async def stats(interaction, name: str):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    row = await container_row(name)

    result = await lxc(
        row[2],
        "info",
        name,
        "--resources",
    )

    await interaction.response.send_message(
        embed=make_embed(
            "📊 VPS Resources",
            codebox(result.output),
        ),
        ephemeral=True,
    )


# ============================================================
# RESOURCE LIMITS
# ============================================================

@bot.tree.command(
    name="limits",
    description="Set CPU/RAM/disk limits",
)
@admin_check()
async def limits(
    interaction,
    name: str,
    ram: Optional[str] = None,
    cpu: Optional[str] = None,
    disk: Optional[str] = None,
):
    row = await container_row(name)

    if not row:
        return await interaction.response.send_message(
            "❌ Container not registered.",
            ephemeral=True,
        )

    node_id = row[2]
    changed = []
    failures = []

    if ram:
        if not re.fullmatch(r"\d+(?:MB|GB)", ram.upper()):
            return await interaction.response.send_message(
                "RAM example: `512MB`, `2GB`.",
                ephemeral=True,
            )

        r = await lxc(
            node_id,
            "config",
            "set",
            name,
            "limits.memory",
            ram.upper(),
        )

        if r.ok:
            changed.append(f"RAM={ram.upper()}")
        else:
            failures.append(r.output)

    if cpu:
        if not re.fullmatch(r"\d+(?:-\d+)?", cpu):
            return await interaction.response.send_message(
                "CPU example: `1`, `2`, `0-3`.",
                ephemeral=True,
            )

        r = await lxc(
            node_id,
            "config",
            "set",
            name,
            "limits.cpu",
            cpu,
        )

        if r.ok:
            changed.append(f"CPU={cpu}")
        else:
            failures.append(r.output)

    if disk:
        if not re.fullmatch(r"\d+(?:MB|GB|TB)", disk.upper()):
            return await interaction.response.send_message(
                "Disk example: `10GB`, `50GB`.",
                ephemeral=True,
            )

        r = await lxc(
            node_id,
            "config",
            "device",
            "set",
            name,
            "root",
            "size",
            disk.upper(),
        )

        if r.ok:
            changed.append(f"Disk={disk.upper()}")
        else:
            failures.append(r.output)

    await audit(
        interaction.user.id,
        "limits",
        name,
        ";".join(changed),
    )

    description = (
        "✅ " + ", ".join(changed)
        if changed
        else "❌ Nothing changed."
    )

    if failures:
        description += "\n\n" + codebox("\n".join(failures), 1800)

    await interaction.response.send_message(
        embed=make_embed(
            "⚙️ Resource Limits",
            description,
        ),
        ephemeral=True,
    )


# ============================================================
# SNAPSHOTS
# ============================================================

@bot.tree.command(
    name="snapshot",
    description="Create an LXC snapshot",
)
async def snapshot(
    interaction,
    name: str,
    snapshot_name: str = "",
):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    row = await container_row(name)

    snapshot_name = snapshot_name.strip() or f"backup-{int(time.time())}"

    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,48}", snapshot_name):
        return await interaction.response.send_message(
            "❌ Invalid snapshot name.",
            ephemeral=True,
        )

    result = await lxc(
        row[2],
        "snapshot",
        name,
        snapshot_name,
        timeout=60,
    )

    await audit(
        interaction.user.id,
        "snapshot",
        name,
        snapshot_name,
    )

    await interaction.response.send_message(
        embed=make_embed(
            "💾 Snapshot",
            (
                f"Snapshot `{snapshot_name}` created."
                if result.ok
                else codebox(result.output)
            ),
            discord.Color.green()
            if result.ok
            else discord.Color.red(),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="snapshots",
    description="List LXC snapshots",
)
async def snapshots(interaction, name: str):
    if not await can_manage(interaction.user.id, name):
        return await interaction.response.send_message(
            "❌ Access denied.",
            ephemeral=True,
        )

    row = await container_row(name)

    result = await lxc(
        row[2],
        "info",
        name,
    )

    text = result.output

    if "Snapshots:" in text:
        text = text[text.index("Snapshots:"):]
    else:
        text = "No snapshot section reported."

    await interaction.response.send_message(
        embed=make_embed(
            "💾 Snapshots",
            codebox(text),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="restore",
    description="Restore a snapshot into a container",
)
@admin_check()
async def restore(
    interaction,
    name: str,
    snapshot_name: str,
):
    row = await container_row(name)

    if not row:
        return await interaction.response.send_message(
            "❌ Container not registered.",
            ephemeral=True,
        )

    view = ConfirmView(interaction.user.id)

    await interaction.response.send_message(
        f"⚠️ Restore `{snapshot_name}` on `{name}`?\n"
        "Current container state may be overwritten.",
        view=view,
        ephemeral=True,
    )

    await view.wait()

    if not view.confirmed:
        return

    result = await lxc(
        row[2],
        "restore",
        name,
        snapshot_name,
        timeout=90,
    )

    await audit(
        interaction.user.id,
        "restore",
        name,
        snapshot_name,
    )

    await interaction.followup.send(
        embed=make_embed(
            "💾 Restore",
            "Snapshot restored." if result.ok else codebox(result.output),
            discord.Color.green()
            if result.ok
            else discord.Color.red(),
        ),
        ephemeral=True,
    )


# ============================================================
# CLONE
# ============================================================

@bot.tree.command(
    name="clone",
    description="Clone a VPS container",
)
@admin_check()
async def clone(
    interaction,
    source: str,
    target: str,
):
    if not valid_name(source) or not valid_name(target):
        return await interaction.response.send_message(
            "❌ Invalid container name.",
            ephemeral=True,
        )

    row = await container_row(source)

    if not row:
        return await interaction.response.send_message(
            "❌ Source container is not registered.",
            ephemeral=True,
        )

    if await container_row(target):
        return await interaction.response.send_message(
            "❌ Target already exists.",
            ephemeral=True,
        )

    result = await lxc(
        row[2],
        "copy",
        source,
        target,
        timeout=120,
    )

    if not result.ok:
        return await interaction.response.send_message(
            embed=make_embed(
                "❌ Clone failed",
                codebox(result.output),
                discord.Color.red(),
            ),
            ephemeral=True,
        )

    await db_exec(
        """INSERT INTO containers
           (name, owner_id, node_id, os, created_at, last_action)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            target,
            interaction.user.id,
            row[2],
            row[3],
            int(time.time()),
            "cloned",
        ),
    )

    await audit(
        interaction.user.id,
        "clone",
        target,
        f"source={source}",
    )

    await interaction.response.send_message(
        embed=make_embed(
            "🧬 Clone complete",
            f"`{source}` → `{target}`",
            discord.Color.green(),
        ),
        ephemeral=True,
    )


# ============================================================
# NODE MANAGEMENT
# ============================================================

@bot.tree.command(
    name="nodes",
    description="Show all LXC nodes",
)
async def nodes(interaction):
    rows = await db_all(
        """SELECT id,name,ip,ssh_user,is_local,online,last_check
           FROM nodes ORDER BY id"""
    )

    lines = []

    for node_id, name, ip, ssh_user, local, online, checked in rows:
        state = "🟢 ONLINE" if online else "🔴 OFFLINE / UNKNOWN"
        kind = "LOCAL" if local else "SSH"
        check = f"<t:{checked}:R>" if checked else "never"

        lines.append(
            f"**#{node_id} {name}**\n"
            f"└ `{ip}` • `{kind}` • `{state}` • checked {check}"
        )

    await interaction.response.send_message(
        embed=make_embed(
            "🌐 Omega Node Fleet",
            "\n\n".join(lines),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="addnode",
    description="Owner/admin: add an external LXC node",
)
@admin_check()
async def addnode(
    interaction,
    name: str,
    ip: str,
    ssh_user: str = "root",
):
    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,32}", name):
        return await interaction.response.send_message(
            "❌ Invalid node name.",
            ephemeral=True,
        )

    if not re.fullmatch(r"[a-zA-Z0-9_.:-]{1,255}", ip):
        return await interaction.response.send_message(
            "❌ Invalid node address.",
            ephemeral=True,
        )

    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,32}", ssh_user):
        return await interaction.response.send_message(
            "❌ Invalid SSH username.",
            ephemeral=True,
        )

    try:
        await db_exec(
            """INSERT INTO nodes
               (name, ip, ssh_user, is_local)
               VALUES (?, ?, ?, 0)""",
            (name, ip, ssh_user),
        )
    except Exception as exc:
        return await interaction.response.send_message(
            embed=make_embed(
                "❌ Node not added",
                str(exc),
                discord.Color.red(),
            ),
            ephemeral=True,
        )

    await audit(
        interaction.user.id,
        "addnode",
        name,
        ip,
    )

    await interaction.response.send_message(
        embed=make_embed(
            "🌐 Node Added",
            f"**{name}** → `{ssh_user}@{ip}`",
            discord.Color.green(),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="removenode",
    description="Admin: remove an external node",
)
@admin_check()
async def removenode(interaction, node: int):
    row = await db_one(
        "SELECT id,name,is_local FROM nodes WHERE id=?",
        (node,),
    )

    if not row:
        return await interaction.response.send_message(
            "❌ Node not found.",
            ephemeral=True,
        )

    if row[2]:
        return await interaction.response.send_message(
            "❌ The default local node cannot be removed.",
            ephemeral=True,
        )

    view = ConfirmView(interaction.user.id)

    await interaction.response.send_message(
        f"⚠️ Remove node **{row[1]}**?\n"
        "Existing containers on that node are not automatically deleted.",
        view=view,
        ephemeral=True,
    )

    await view.wait()

    if not view.confirmed:
        return

    await db_exec(
        "DELETE FROM nodes WHERE id=?",
        (node,),
    )

    await audit(
        interaction.user.id,
        "removenode",
        row[1],
    )

    await interaction.followup.send(
        f"🗑️ Removed node `{row[1]}`.",
        ephemeral=True,
    )


@bot.tree.command(
    name="nodeinfo",
    description="Show LXD server information",
)
@admin_check()
async def nodeinfo(interaction, node: int = 1):
    result = await lxc(
        node,
        "info",
        timeout=20,
    )

    await interaction.response.send_message(
        embed=make_embed(
            "🖥️ Node Information",
            codebox(result.output),
            discord.Color.green()
            if result.ok
            else discord.Color.red(),
        ),
        ephemeral=True,
    )


# ============================================================
# ADMIN MANAGEMENT
# ============================================================

@bot.tree.command(
    name="addadmin",
    description="Owner only: add a deployment administrator",
)
async def addadmin(interaction, user: discord.User):
    if interaction.user.id != OWNER_ID:
        return await interaction.response.send_message(
            "👑 **Owner only.**",
            ephemeral=True,
        )

    await db_exec(
        "INSERT OR IGNORE INTO admins(user_id) VALUES(?)",
        (user.id,),
    )

    await audit(
        interaction.user.id,
        "addadmin",
        str(user.id),
    )

    await interaction.response.send_message(
        embed=make_embed(
            "👑 Admin Added",
            f"{user.mention} can now deploy and manage hosting resources.",
            discord.Color.gold(),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="removeadmin",
    description="Owner only: remove a deployment administrator",
)
async def removeadmin(interaction, user: discord.User):
    if interaction.user.id != OWNER_ID:
        return await interaction.response.send_message(
            "👑 **Owner only.**",
            ephemeral=True,
        )

    if user.id == OWNER_ID:
        return await interaction.response.send_message(
            "❌ The owner cannot be removed.",
            ephemeral=True,
        )

    await db_exec(
        "DELETE FROM admins WHERE user_id=?",
        (user.id,),
    )

    await audit(
        interaction.user.id,
        "removeadmin",
        str(user.id),
    )

    await interaction.response.send_message(
        f"🗑️ Removed {user.mention} from hosting admins.",
        ephemeral=True,
    )


@bot.tree.command(
    name="admins",
    description="Owner: list hosting administrators",
)
async def admins(interaction):
    if interaction.user.id != OWNER_ID:
        return await interaction.response.send_message(
            "👑 **Owner only.**",
            ephemeral=True,
        )

    rows = await db_all(
        "SELECT user_id FROM admins ORDER BY user_id"
    )

    text = "\n".join(
        f"• <@{uid}> (`{uid}`)"
        for (uid,) in rows
    )

    await interaction.response.send_message(
        embed=make_embed(
            "🛡️ Hosting Administrators",
            text or "No additional admins.",
            discord.Color.gold(),
        ),
        ephemeral=True,
    )


@bot.tree.command(
    name="audit",
    description="Admin: view infrastructure audit log",
)
@admin_check()
async def audit_command(interaction, limit: int = 20):
    limit = max(1, min(limit, 50))

    rows = await db_all(
        """SELECT user_id,action,target,details,created_at
           FROM audit_log
           ORDER BY id DESC LIMIT ?""",
        (limit,),
    )

    lines = []

    for uid, action, target, details, created in rows:
        lines.append(
            f"<t:{created}:R> • <@{uid}> • "
            f"`{action}` • `{target or '-'}`"
        )

    await interaction.response.send_message(
        embed=make_embed(
            "📜 Infrastructure Audit",
            "\n".join(lines) or "No events recorded.",
            discord.Color.dark_grey(),
        ),
        ephemeral=True,
    )


# ============================================================
# BOT STATS
# ============================================================

@bot.tree.command(
    name="status",
    description="Show Omega hosting bot status",
)
async def status(interaction):
    node_count = (await db_one("SELECT COUNT(*) FROM nodes"))[0]
    container_count = (await db_one("SELECT COUNT(*) FROM containers"))[0]
    admin_count = (await db_one("SELECT COUNT(*) FROM admins"))[0]

    uptime = int(time.monotonic() - bot.started)

    e = make_embed(
        "⚡ Omega Hosting Status",
        "The Discord control plane is operational.",
        discord.Color.green(),
    )

    e.add_field(
        name="Discord latency",
        value=f"`{round(bot.latency * 1000)} ms`",
        inline=True,
    )
    e.add_field(
        name="Nodes",
        value=f"`{node_count}`",
        inline=True,
    )
    e.add_field(
        name="Containers",
        value=f"`{container_count}`",
        inline=True,
    )
    e.add_field(
        name="Admins",
        value=f"`{admin_count}`",
        inline=True,
    )
    e.add_field(
        name="Uptime",
        value=f"`{uptime}s`",
        inline=True,
    )
    e.add_field(
        name="Owner",
        value=f"<@{OWNER_ID}>",
        inline=True,
    )

    await interaction.response.send_message(e)


# ============================================================
# PREFIX COMMAND
# !manage <container>
# ============================================================

@bot.command(name="manage")
async def prefix_manage(ctx, name: Optional[str] = None):
    if not name:
        return await ctx.send(
            embed=make_embed(
                "🎛️ Omega Manage",
                f"Usage: `{PREFIX}manage <container>`",
            )
        )

    if not await can_manage(ctx.author.id, name):
        return await ctx.send(
            embed=make_embed(
                "❌ Access denied",
                "You don't own this VPS and are not a hosting admin.",
                discord.Color.red(),
            )
        )

    row = await container_row(name)

    if not row:
        return await ctx.send(
            embed=make_embed(
                "❌ Not found",
                "This container is not registered.",
                discord.Color.red(),
            )
        )

    cname, owner_id, node_id, os_name = row

    status, info = await container_status(
        node_id,
        cname,
    )

    ip = await container_ip(
        node_id,
        cname,
    )

    e = make_embed(
        f"🎛️ {cname}",
        "## Omega LXC Control Center\n"
        "Use the buttons to control your VPS.",
        discord.Color.green()
        if status == "RUNNING"
        else discord.Color.red(),
    )

    e.add_field(name="Status", value=status_badge(status), inline=True)
    e.add_field(name="IPv4", value=f"`{ip}`", inline=True)
    e.add_field(name="OS", value=f"`{os_name}`", inline=True)
    e.add_field(name="Node", value=f"`{node_id}`", inline=True)
    e.add_field(name="Owner", value=f"<@{owner_id}>", inline=True)
    e.add_field(
        name="Output",
        value=codebox(info, 1300),
        inline=False,
    )
    e.set_footer(text="Omega Hosting • !manage")

    await ctx.send(
        embed=e,
        view=ManageView(
            cname,
            node_id,
            owner_id,
        ),
    )


# ============================================================
# COMMAND ERROR HANDLER
# ============================================================

@bot.tree.error
async def slash_error(interaction, error):
    print("[slash-error]", repr(error))

    if isinstance(error, app_commands.CheckFailure):
        return

    message = "❌ An unexpected error occurred."

    if interaction.response.is_done():
        await interaction.followup.send(
            message,
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            message,
            ephemeral=True,
        )


# ============================================================
# STARTUP
# ============================================================

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "DISCORD_TOKEN is not set. "
            "Export it before starting the bot."
        )

    asyncio.run(init_db())
    bot.run(TOKEN)
