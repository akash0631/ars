"""
API v1 Router - Aggregates all endpoint routers
"""
from fastapi import APIRouter

# Phase 1: Auth, RBAC, RLS, Audit
from app.api.v1.endpoints.auth import router as auth_router
from app.api.v1.endpoints.users import router as users_router
from app.api.v1.endpoints.roles import router as roles_router
from app.api.v1.endpoints.rls import router as rls_router
from app.api.v1.endpoints.audit import router as audit_router

# Phase 2: Table Management, Upsert, Upload
from app.api.v1.endpoints.tables import router as tables_router
from app.api.v1.endpoints.data_ops import router as data_ops_router
from app.api.v1.endpoints.upload import router as upload_router

# Phase 3: Allocation Engine
from app.api.v1.endpoints.allocations import router as allocations_router

# Phase 4: MSA Stock Calculation
from app.api.v1.endpoints.msa_stock import router as msa_router
from app.api.v1.endpoints.msa import router as msa_legacy_router

# Phase 4b: Contribution Percentage Analysis
from app.api.v1.endpoints.contribution import router as contribution_router

# Phase 4c: BDC Creation
from app.api.v1.endpoints.bdc import router as bdc_router

# Phase 5: Settings
from app.api.v1.endpoints.settings import router as settings_router

# Phase 6b: Store Stock / Data Validation
from app.api.v1.endpoints.store_stock import router as store_stock_router
from app.api.v1.endpoints.grid_builder import router as grid_builder_router

# Phase 7: Lookup Art Master
from app.api.v1.endpoints.lookup_art_master import router as lookup_art_master_router

# Phase 6: Dashboard
from app.api.v1.endpoints.dashboard import router as dashboard_router

api_router = APIRouter(prefix="/api/v1")

# Phase 1
api_router.include_router(auth_router)
api_router.include_router(users_router)
api_router.include_router(roles_router)
api_router.include_router(rls_router)
api_router.include_router(audit_router)

# Phase 2
api_router.include_router(tables_router)
api_router.include_router(data_ops_router)
api_router.include_router(upload_router)

# Phase 3
api_router.include_router(allocations_router)

# Phase 4: MSA Stock Calculation
api_router.include_router(msa_router)
api_router.include_router(msa_legacy_router)

# Phase 4b: Contribution Percentage Analysis
api_router.include_router(contribution_router)

# Phase 4c: BDC Creation
api_router.include_router(bdc_router)

# Phase 5
api_router.include_router(settings_router)

# Phase 6b: Store Stock / Data Preparation
api_router.include_router(store_stock_router)
api_router.include_router(grid_builder_router)

# Phase 7: Lookup Art Master
api_router.include_router(lookup_art_master_router)

# Phase 6
api_router.include_router(dashboard_router)
