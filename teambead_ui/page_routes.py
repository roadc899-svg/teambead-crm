from teambead_domains.analytics import bind_domain_routes as bind_analytics_routes
from teambead_domains.auth import bind_domain_routes as bind_auth_routes
from teambead_domains.management import bind_domain_routes as bind_management_routes
from teambead_domains.parsers import bind_domain_routes as bind_parser_routes
from teambead_domains.reports import bind_domain_routes as bind_report_routes


def bind_page_routes(ctx):
    routes = {}
    for binder in (
        bind_analytics_routes,
        bind_auth_routes,
        bind_parser_routes,
        bind_management_routes,
        bind_report_routes,
    ):
        routes.update(binder(ctx))
    return routes
