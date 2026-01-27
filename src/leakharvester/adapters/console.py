from rich.console import Console
from rich.theme import Theme

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
    console.print(f"[error]ERROR:[/error] {msg}")

def log_success(msg: str) -> None:
    console.print(f"[success]SUCCESS:[/success] {msg}")
