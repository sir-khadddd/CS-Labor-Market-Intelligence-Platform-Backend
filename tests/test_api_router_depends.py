"""Verify job, skill, and salary routers inject Postgres via Depends."""

import inspect

from fastapi.routing import APIRoute

from api.dependencies import get_postgres_connection
from api.routers import job_demand, salary, skill_demand

ROUTER_MODULES = (job_demand, skill_demand, salary)


def _depends_on_postgres(route: APIRoute) -> bool:
    stack = [route.dependant]
    while stack:
        dependant = stack.pop()
        for dep in dependant.dependencies:
            if dep.call is get_postgres_connection:
                return True
            stack.append(dep)
    return False


def _signature_declares_postgres_depends(route: APIRoute) -> bool:
    signature = inspect.signature(route.endpoint)
    conn_param = signature.parameters.get("conn")
    if conn_param is None:
        return False
    default = conn_param.default
    dependency = getattr(default, "dependency", None)
    return dependency is get_postgres_connection


def test_job_skill_salary_routes_use_postgres_depends():
    missing = []
    for module in ROUTER_MODULES:
        routes = [route for route in module.router.routes if isinstance(route, APIRoute)]
        assert routes, f"expected routes on {module.__name__}"
        for route in routes:
            if not (_depends_on_postgres(route) and _signature_declares_postgres_depends(route)):
                missing.append(f"{module.__name__}:{route.path}")

    assert not missing, f"routes missing get_postgres_connection Depends: {missing}"
