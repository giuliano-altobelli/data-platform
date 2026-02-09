from databricks.bundles.core import (
    Bundle,
    Resources,
    load_resources_from_current_package_module,
)


def load_resources(_bundle: Bundle) -> Resources:
    """Load all Python-defined resources in this package."""
    return load_resources_from_current_package_module()
