use crate::entry_def::EntryDefStoreKey;
use crate::prelude::SignedValidationReceipt;
use crate::query::to_blob;
use crate::scratch::Scratch;
use crate::validation_db::ValidationLimboStatus;
use holo_hash::encode::blake2b_256;
use holo_hash::*;
use holochain_sqlite::rusqlite::named_params;
use holochain_sqlite::rusqlite::Transaction;
use holochain_types::dht_op::DhtOpHashed;
use holochain_types::dht_op::DhtOpLight;
use holochain_types::dht_op::OpOrder;
use holochain_types::prelude::DhtOpError;
use holochain_types::prelude::DnaDefHashed;
use holochain_types::prelude::DnaWasmHashed;
use holochain_types::EntryHashed;
use holochain_zome_types::*;

pub use error::*;

mod error;

#[macro_export]
macro_rules! sql_insert {
    ($txn:expr, $table:ident, { $($field:literal : $val:expr , )+ $(,)? }) => {{
        let table = stringify!($table);
        let fieldnames = &[ $( { $field } ,)+ ].join(",");
        let fieldvars = &[ $( { format!(":{}", $field) } ,)+ ].join(",");
        let sql = format!("INSERT INTO {} ({}) VALUES ({})", table, fieldnames, fieldvars);
        $txn.execute(&sql, &[$(
            (format!(":{}", $field).as_str(), &$val as &dyn holochain_sqlite::rusqlite::ToSql),
        )+])
    }};
}

macro_rules! dht_op_update {
    ($txn:expr, $hash:expr, { $($field:literal : $val:expr , )+ $(,)? }) => {{
        let fieldvars = &[ $( { format!("{} = :{}", $field, $field) } ,)+ ].join(",");
        let sql = format!(
            "
            UPDATE DhtOp 
            SET {}
            WHERE DhtOp.hash = :hash
            ", fieldvars);
        $txn.execute(&sql, &[
            (":hash", &$hash as &dyn holochain_sqlite::rusqlite::ToSql),
            $(
            (format!(":{}", $field).as_str(), &$val as &dyn holochain_sqlite::rusqlite::ToSql),
        )+])
    }};
}

/// Insert a [`DhtOp`] into the [`Scratch`].
pub fn insert_op_scratch(scratch: &mut Scratch, op: DhtOpHashed) -> StateMutationResult<()> {
    let (op, _) = op.into_inner();
    let op_light = op.to_light();
    let header = op.header();
    let signature = op.signature().clone();
    if let Some(entry) = op.entry() {
        let entry_hashed = EntryHashed::with_pre_hashed(
            entry.clone(),
            header
                .entry_hash()
                .ok_or_else(|| DhtOpError::HeaderWithoutEntry(header.clone()))?
                .clone(),
        );
        scratch.add_entry(entry_hashed);
    }
    let header_hashed = HeaderHashed::with_pre_hashed(header, op_light.header_hash().to_owned());
    let header_hashed = SignedHeaderHashed::with_presigned(header_hashed, signature);
    scratch.add_header(header_hashed);
    Ok(())
}

pub fn insert_element_scratch(scratch: &mut Scratch, element: Element) {
    let (header, entry) = element.into_inner();
    scratch.add_header(header);
    if let Some(entry) = entry.into_option() {
        scratch.add_entry(EntryHashed::from_content_sync(entry))
    }
}

/// Insert a [`DhtOp`] into the database.
pub fn insert_op(
    txn: &mut Transaction,
    op: DhtOpHashed,
    is_authored: bool,
) -> StateMutationResult<()> {
    let (op, hash) = op.into_inner();
    let op_light = op.to_light();
    let header = op.header();
    let timestamp = header.timestamp();
    let signature = op.signature().clone();
    if let Some(entry) = op.entry() {
        let entry_hashed = EntryHashed::with_pre_hashed(
            entry.clone(),
            header
                .entry_hash()
                .ok_or_else(|| DhtOpError::HeaderWithoutEntry(header.clone()))?
                .clone(),
        );
        insert_entry(txn, entry_hashed)?;
    }
    let header_hashed = HeaderHashed::with_pre_hashed(header, op_light.header_hash().to_owned());
    let header_hashed = SignedHeaderHashed::with_presigned(header_hashed, signature);
    let op_order = OpOrder::new(op_light.get_type(), header_hashed.header().timestamp());
    insert_header(txn, header_hashed)?;
    insert_op_lite(txn, op_light, hash, is_authored, op_order, timestamp)?;
    Ok(())
}

/// Insert a [`DhtOpLight`] into the database.
pub fn insert_op_lite(
    txn: &mut Transaction,
    op_lite: DhtOpLight,
    hash: DhtOpHash,
    is_authored: bool,
    order: OpOrder,
    timestamp: Timestamp,
) -> StateMutationResult<()> {
    let header_hash = op_lite.header_hash().clone();
    let basis = op_lite.dht_basis().to_owned();
    sql_insert!(txn, DhtOp, {
        "hash": hash,
        "type": op_lite.get_type(),
        "storage_center_loc": basis.get_loc(),
        "authored_timestamp_ms": timestamp.to_sql_ms_lossy(),
        "basis_hash": basis,
        "header_hash": header_hash,
        "is_authored": is_authored,
        "require_receipt": 0,
        "blob": to_blob(op_lite)?,
        "op_order": order,
    })?;
    Ok(())
}

/// Insert a [`SignedValidationReceipt`] into the database.
pub fn insert_validation_receipt(
    txn: &mut Transaction,
    receipt: SignedValidationReceipt,
) -> StateMutationResult<()> {
    let op_hash = receipt.receipt.dht_op_hash.clone();
    let bytes: UnsafeBytes = SerializedBytes::try_from(receipt)?.into();
    let bytes: Vec<u8> = bytes.into();
    let hash = blake2b_256(&bytes);
    sql_insert!(txn, ValidationReceipt, {
        "hash": hash,
        "op_hash": op_hash,
        "blob": bytes,
    })?;
    Ok(())
}

/// Insert a [`DnaWasm`] into the database.
pub fn insert_wasm(txn: &mut Transaction, wasm: DnaWasmHashed) -> StateMutationResult<()> {
    let (wasm, hash) = wasm.into_inner();
    sql_insert!(txn, Wasm, {
        "hash": hash,
        "blob": to_blob(wasm)?,
    })?;
    Ok(())
}

/// Insert a [`DnaDef`] into the database.
pub fn insert_dna_def(txn: &mut Transaction, dna_def: DnaDefHashed) -> StateMutationResult<()> {
    let (dna_def, hash) = dna_def.into_inner();
    sql_insert!(txn, DnaDef, {
        "hash": hash,
        "blob": to_blob(dna_def)?,
    })?;
    Ok(())
}

/// Insert a [`EntryDef`] into the database.
pub fn insert_entry_def(
    txn: &mut Transaction,
    key: EntryDefStoreKey,
    entry_def: EntryDef,
) -> StateMutationResult<()> {
    sql_insert!(txn, EntryDef, {
        "key": key,
        "blob": to_blob(entry_def)?,
    })?;
    Ok(())
}

/// Insert [`ConductorState`] into the database.
pub fn insert_conductor_state(
    txn: &mut Transaction,
    bytes: SerializedBytes,
) -> StateMutationResult<()> {
    let bytes: Vec<u8> = UnsafeBytes::from(bytes).into();
    sql_insert!(txn, ConductorState, {
        "id": 1,
        "blob": bytes,
    })?;
    Ok(())
}

/// Set the validation status of a [`DhtOp`] in the database.
pub fn set_validation_status(
    txn: &mut Transaction,
    hash: DhtOpHash,
    status: ValidationStatus,
) -> StateMutationResult<()> {
    dht_op_update!(txn, hash, {
        "validation_status": status,
    })?;
    Ok(())
}

/// Set the whether or not a receipt is required of a [`DhtOp`] in the database.
pub fn set_require_receipt(
    txn: &mut Transaction,
    hash: DhtOpHash,
    require_receipt: bool,
) -> StateMutationResult<()> {
    dht_op_update!(txn, hash, {
        "require_receipt": require_receipt,
    })?;
    Ok(())
}

/// Set the validation stage of a [`DhtOp`] in the database.
pub fn set_validation_stage(
    txn: &mut Transaction,
    hash: DhtOpHash,
    status: ValidationLimboStatus,
) -> StateMutationResult<()> {
    let stage = match status {
        ValidationLimboStatus::Pending => None,
        ValidationLimboStatus::AwaitingSysDeps(_) => Some(0),
        ValidationLimboStatus::SysValidated => Some(1),
        ValidationLimboStatus::AwaitingAppDeps(_) => Some(2),
        ValidationLimboStatus::AwaitingIntegration => Some(3),
    };
    let now = holochain_types::timestamp::now().0;
    txn.execute(
        "
        UPDATE DhtOp
        SET
        num_validation_attempts = IFNULL(num_validation_attempts, 0) + 1,
        last_validation_attempt = :last_validation_attempt,
        validation_stage = :validation_stage
        WHERE
        DhtOp.hash = :hash
        ",
        named_params! {
            ":last_validation_attempt": now,
            ":validation_stage": stage,
            ":hash": hash,
        },
    )?;
    Ok(())
}

/// Set when a [`DhtOp`] was integrated.
pub fn set_when_integrated(
    txn: &mut Transaction,
    hash: DhtOpHash,
    time: Timestamp,
) -> StateMutationResult<()> {
    dht_op_update!(txn, hash, {
        "when_integrated_ns": to_blob(time)?,
        "when_integrated": time,
    })?;
    Ok(())
}

/// Set when a [`DhtOp`] was last publish time
pub fn set_last_publish_time(
    txn: &mut Transaction,
    hash: DhtOpHash,
    unix_epoch: std::time::Duration,
) -> StateMutationResult<()> {
    dht_op_update!(txn, hash, {
        "last_publish_time": unix_epoch.as_secs(),
    })?;
    Ok(())
}

/// Set the receipt count for a [`DhtOp`].
pub fn set_receipt_count(
    txn: &mut Transaction,
    hash: DhtOpHash,
    count: u32,
) -> StateMutationResult<()> {
    dht_op_update!(txn, hash, {
        "receipt_count": count,
    })?;
    Ok(())
}

/// Add one to the receipt count for a [`DhtOp`].
pub fn add_one_receipt_count(txn: &mut Transaction, hash: &DhtOpHash) -> StateMutationResult<()> {
    txn.execute(
        "UPDATE DhtOp SET receipt_count = IFNULL(receipt_count, 0) + 1 WHERE hash = :hash;",
        named_params! { ":hash": hash },
    )?;
    Ok(())
}

/// Insert a [`Header`] into the database.
pub fn insert_header(txn: &mut Transaction, header: SignedHeaderHashed) -> StateMutationResult<()> {
    let (header, signature) = header.into_header_and_signature();
    let (header, hash) = header.into_inner();
    let header_type = header.header_type();
    let header_seq = header.header_seq();
    let author = header.author().clone();
    let prev_hash = header.prev_header().cloned();
    let private = match header.entry_type().map(|et| et.visibility()) {
        Some(EntryVisibility::Private) => true,
        Some(EntryVisibility::Public) => false,
        None => false,
    };
    match header {
        Header::CreateLink(create_link) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "base_hash": create_link.base_address,
                "zome_id": create_link.zome_id.index() as u32,
                "tag": create_link.tag,
                "blob": to_blob(SignedHeader::from((Header::CreateLink(create_link.clone()), signature)))?,
            })?;
        }
        Header::DeleteLink(delete_link) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "create_link_hash": delete_link.link_add_address,
                "blob": to_blob(SignedHeader::from((Header::DeleteLink(delete_link.clone()), signature)))?,
            })?;
        }
        Header::Create(create) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "entry_hash": create.entry_hash,
                "entry_type": create.entry_type,
                "private_entry": private,
                "blob": to_blob(SignedHeader::from((Header::Create(create.clone()), signature)))?,
            })?;
        }
        Header::Delete(delete) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "deletes_entry_hash": delete.deletes_entry_address,
                "deletes_header_hash": delete.deletes_address,
                "blob": to_blob(SignedHeader::from((Header::Delete(delete.clone()), signature)))?,
            })?;
        }
        Header::Update(update) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "entry_hash": update.entry_hash,
                "entry_type": update.entry_type,
                "original_entry_hash": update.original_entry_address,
                "original_header_hash": update.original_header_address,
                "private_entry": private,
                "blob": to_blob(SignedHeader::from((Header::Update(update.clone()), signature)))?,
            })?;
        }
        Header::InitZomesComplete(izc) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "blob": to_blob(SignedHeader::from((Header::InitZomesComplete(izc), signature)))?,
            })?;
        }
        Header::Dna(dna) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "blob": to_blob(SignedHeader::from((Header::Dna(dna), signature)))?,
            })?;
        }
        Header::AgentValidationPkg(avp) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "blob": to_blob(SignedHeader::from((Header::AgentValidationPkg(avp), signature)))?,
            })?;
        }
        Header::OpenChain(open) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "blob": to_blob(SignedHeader::from((Header::OpenChain(open), signature)))?,
            })?;
        }
        Header::CloseChain(close) => {
            sql_insert!(txn, Header, {
                "hash": hash,
                "type": header_type ,
                "seq": header_seq,
                "author": author,
                "prev_hash": prev_hash,
                "blob": to_blob(SignedHeader::from((Header::CloseChain(close), signature)))?,
            })?;
        }
    }
    Ok(())
}

/// Insert an [`Entry`] into the database.
pub fn insert_entry(txn: &mut Transaction, entry: EntryHashed) -> StateMutationResult<()> {
    let (entry, hash) = entry.into_inner();
    let mut cap_secret = None;
    let mut cap_access = None;
    let mut cap_grantor = None;
    let cap_tag = match &entry {
        Entry::CapGrant(ZomeCallCapGrant {
            tag,
            access,
            functions: _,
        }) => {
            cap_access = match access {
                CapAccess::Unrestricted => Some("unrestricted"),
                CapAccess::Transferable { secret } => {
                    cap_secret = Some(to_blob(secret)?);
                    Some("transferable")
                }
                CapAccess::Assigned {
                    secret,
                    assignees: _,
                } => {
                    cap_secret = Some(to_blob(secret)?);
                    // TODO: put assignees in when we merge in BHashSet from develop.
                    Some("assigned")
                }
            };
            // TODO: put functions in when we merge in BHashSet from develop.
            Some(tag.clone())
        }
        Entry::CapClaim(CapClaim {
            tag,
            grantor,
            secret,
        }) => {
            cap_secret = Some(to_blob(secret)?);
            cap_grantor = Some(grantor.clone());
            Some(tag.clone())
        }
        _ => None,
    };
    sql_insert!(txn, Entry, {
        "hash": hash,
        "blob": to_blob(entry)?,
        "tag": cap_tag,
        "access_type": cap_access,
        "grantor": cap_grantor,
        "cap_secret": cap_secret,
        // TODO: add cap functions and assignees
    })?;
    Ok(())
}
