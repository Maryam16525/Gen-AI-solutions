import time

import streamlit as st

from utilities import auth, st_logging, templates

logger = st_logging.getLogger(__name__)


def include_page(page, user):
    """Check if the page should be included in the menu, based on the user's roles and the page's public setting. Logic depends on where site is running."""

    if SITE == "local":
        # Always show all pages on local site
        return True

    if SITE == "development":
        # Show all pages if the user have access
        return auth.has_access(page, user)

    if SITE == "production" or SITE == "staging":
        # Show public pages if the user have access
        return page.get("public", False) and auth.has_access(page, user)

    return False


def build_navigation(page_configs, user_info):
    """Builds the navigation based on the page configs and the user's roles"""
    pages = []
    for page in page_configs:
        if include_page(page, user_info):
            st_page = st.Page(
                page["path"],
                title=page.get("name"),
                icon=page.get("icon"),
                url_path=page.get("url_path", None),
                default=page.get("default", False),
            )
            pages.append(st_page)
            page["url_path"] = st_page.url_path
    return pages


# Start of the main script
user_info = auth.authenticate()  # Check that user is authenticated
SITE = auth.instance_info()  # Get the site name (local, development, staging, production)

# increase verbosity of logging if user is dev or we are running locally
if SITE == "local" or SITE == "development" or "gnpt_dev" in user_info['roles']:
    st.set_option("client.showErrorDetails", "full")
    
page_configs = templates.load_page_configs()  # Load all page configs
pages = build_navigation(page_configs, user_info)  # Build the navigation based on the user's roles

# Show the navigation and get selected page
page = st.navigation(pages)

# Find the corresponding page_config for the selected page
page_config = next(
    (
        p
        for p in page_configs
        if p.get("url_path", None) == page.url_path or (p.get("default", False) and (page.url_path == ""))
    ),
    None,
)

if page_config is None:
    # We should never end here...
    logger.error(f"Could not find page for url_path: {page.url_path}")
    st.warning("Unknown error. Please try again later")
    st.stop()

# Set the page title, icon and layout and execute the page

templates.header(page_config)

page.run()
