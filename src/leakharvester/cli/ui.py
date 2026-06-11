import typer.rich_utils
import rich.table
import rich.panel
import rich.console

class AdaptiveTable(rich.table.Table):
    """
    A Rich Table subclass that forces 'expand=False' by default.
    This prevents tables in Typer help output from stretching across the full terminal width.
    """
    def __init__(self, *args, **kwargs):
        kwargs["expand"] = False
        super().__init__(*args, **kwargs)

class AdaptivePanel(rich.panel.Panel):
    """
    A Rich Panel subclass that forces 'expand=False' by default.
    This prevents panels in Typer help output from stretching across the full terminal width.
    """
    def __init__(self, *args, **kwargs):
        kwargs["expand"] = False
        super().__init__(*args, **kwargs)

class AdapterGroup(rich.console.Group):
    """
    A Rich Group subclass tailored for Typer compatibility.
    It handles variable arguments for renderables to work seamlessly with Typer's layout logic.
    """
    def __init__(self, renderables=None, *args, **kwargs):
        # Handle case where renderables might be passed as first arg or not
        # Typer calls Columns(items) -> renderables is list
        if renderables is None:
             full_args = []
        else:
             full_args = renderables if isinstance(renderables, (list, tuple)) else [renderables]
        super().__init__(*full_args)

def configure_typer_ui():
    """
    Applies custom UI configurations to Typer's Rich utilities.
    
    This function monkeypatches Typer's internal references to Rich components
    (Table, Panel, Columns) to use our custom adaptive versions. This ensures
    that CLI help output uses compact, non-expanded tables and panels.
    """
    typer.rich_utils.Table = AdaptiveTable
    typer.rich_utils.Panel = AdaptivePanel
    typer.rich_utils.Columns = AdapterGroup
