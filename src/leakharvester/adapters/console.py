from rich.console import Console
from rich.theme import Theme
from rich.panel import Panel

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green"
})

console = Console(theme=custom_theme)

def log_info(msg: str) -> None:
    console.print(f"[info]INFO:[/info] {msg}")

def log_warning(msg: str) -> None:
    console.print(f"[warning]WARNING:[/warning] {msg}")

def log_error(msg: str) -> None:
    console.print(Panel(msg, title="[bold red]ERROR[/bold red]", border_style="red", expand=False))

def log_success(msg: str) -> None:
    console.print(f"[success]SUCCESS:[/success] {msg}")
