""" FastAPI microservice router providing transaction services """

from __future__ import annotations

import fastapi


from core import (common_ids, transactions, errors)
from frontend import sessions, user_auth


router = fastapi.APIRouter()


@router.post("/transaction/{trxid}/begin")
async def trx_begin(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    print(f"/transaction/{trxid}/begin")
    trx = transactions.Transaction.get_or_create_transaction_with_id(trxid=trxid, for_user=session.user)
    return trx.trxid


@router.post("/transaction/{trxid}/commit")
async def trx_commit(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    trx = transactions.Transaction.get_or_raise(trxid)
    await trx.commit()
    return trx.trxid


@router.post("/transaction/{trxid}/abort")
async def trx_abort(
    trxid: common_ids.TrxId,
    session: sessions.Session = fastapi.Depends(user_auth.get_current_session),

) -> common_ids.TrxId:
    trx = transactions.Transaction.get_or_raise(trxid)
    await trx.abort()
    return trx.trxid
