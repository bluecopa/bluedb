# bluedb-ledger Phase G — imported events + id_already_failed

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Implement TigerBeetle `flags.imported` (user-supplied timestamps for replaying historical events) and `id_already_failed` (a failed transfer id is permanently burned — retry returns `id_already_failed`). This completes the create-path surface, so the transitional `NotImplementedYet` / `Gated` are removed.

**Architecture:** A batch is all-imported or all-non-imported, decided by the first event (`imported_event_expected`/`_not_expected`). Imported events carry a user timestamp validated for range, not-advance (≤ cluster now), not-regress (> last assigned), postdate-accounts (transfers), and timeout-zero; the engine uses that timestamp instead of assigning one. Any transfer whose result is a failure (including the transient set and `linked_event_failed`) has its id recorded in a persistent failure index (tag 0x14); a later attempt with that id returns `id_already_failed`.

**Reference:** spec §3 (transient/terminal), §9, §10; TB create_transfers/create_accounts imported codes + id_already_failed (fetched 2026-06-14).

## Pinned semantics
- **id_already_failed (CORRECTED vs TB source):** ONLY the transient codes (`debit_account_not_found`, `credit_account_not_found`, `pending_transfer_not_found`, `exceeds_credits`, `exceeds_debits`, `debit_account_already_closed`, `credit_account_already_closed`) BURN the id. TB burns them because their outcome depends on point-in-time state, so a retry must not diverge — the id is locked to "failed" and the operation must be resubmitted under a new id. **Terminal (deterministic) failures and `linked_event_failed` do NOT burn** (identical retry re-fails the same way; corrected/unchained retry is allowed). `created` and the `exists`/`exists_with_different_*` family NEVER burn. Burned id on a later attempt → `id_already_failed` (code 24, checked after the exists family at 23, before flags 25). Transfers only (accounts have no `id_already_failed`). *(An earlier draft of this plan said terminal/linked also burn — that was wrong; `transfer_burns_id` matches `tigerbeetle.zig::transient()`.)*
- **imported batch consistency:** decided by the first event's `imported` flag. A mismatching event → `imported_event_expected` (first imported, this not) / `imported_event_not_expected` (first not, this is). Empty batch → non-imported.
- **imported timestamp (per event, user-supplied):** `imported_event_timestamp_out_of_range` (must be `0 < ts < 2^63`); `_must_not_advance` (`ts <= now`); `_must_not_regress` (`ts > last_assigned`, i.e. > running max seeded from the watermark); transfers also `_must_postdate_debit_account` (`ts > debit.timestamp`), `_must_postdate_credit_account` (`ts > credit.timestamp`), `imported_event_timeout_must_be_zero`. Non-imported event: `timestamp_must_be_zero` (existing). The engine assigns the user `ts` for imported events (and advances the watermark to it); non-imported events get `max(last+1, now)` as before.
- Imported composes with every transfer kind and with linked.

## Order placement
- Pre-existence (in `validate_*_pre_existence`, given `batch_imported`, `now`): 4/5 batch consistency → (imported ? 7 range, 8 advance : 6 must_be_zero) → 9 reserved_flag/reserved_field → 10/11 id.
- Apply path (given `imported_ts: Option<u64>`, `last_ts`): after account resolution + ledger agreement (and the resolution checks for post/void), before closed/overflow: 54 regress (`ts > last_ts`) → 55/56 postdate (`ts > debit/credit.timestamp`) → 57 timeout-zero. Accounts: 27 regress only (after post-existence).
- id_already_failed (24): in `process_transfer`, after the committed/staged existence check, before `validate_transfer_post_existence`.

## Task 1: keyspace + store (failure index)
- [ ] keyspace: `const TAG_FAILED: u8 = TAG_EXTERNAL_BASE + 4; // 0x14`; `fn failed_key(&self, id: u128) -> Vec<u8>` = `external_key(TAG_FAILED, &id.to_be_bytes())`. Test distinctness.
- [ ] store: `async fn is_failed(substrate, ks, id) -> Result<bool>` = `substrate.get(&ks.failed_key(id)).await?.is_some()`.

## Task 2: model — imported validation, un-gate, burns_id, remove transitional
- [ ] `validate_account_pre_existence(a, batch_imported: bool, now: u64)` and `validate_transfer_pre_existence(t, batch_imported, now)`: add the 4/5 + 6/7/8 logic (per pinned semantics). Keep 9–12 after.
- [ ] `classify`: remove the `IMPORTED → Gated` branch. Now nothing is gated → remove `TransferOp::Gated`. (A regular imported transfer → Regular, imported pending → PendingReserve, etc.)
- [ ] Remove `account_is_gated` (imported accounts now handled).
- [ ] Add `is_imported_batch(first_flags_has_imported: bool) -> bool` — trivial; or compute inline in ledger.
- [ ] Add `pub(crate) fn transfer_burns_id(r: CreateTransferResult) -> bool` = `!matches!(r, Created | Exists | ExistsWithDifferent*)` (list the exists-family variants).
- [ ] Remove `CreateTransferResult::NotImplementedYet` and `CreateAccountResult::NotImplementedYet` (no longer produced).
- [ ] Add a helper to clear nothing — n/a. Add `account regress` handled in ledger.
- [ ] Update `classify_ops` test (imported → Regular now); add imported-validation unit tests; add `transfer_burns_id` unit test.

## Task 3: ledger — imported timestamps + id_already_failed
- [ ] `create_accounts`: compute `let batch_imported = specs.first().is_some_and(|a| a.flags.contains(AccountFlags::IMPORTED));`. Pass `batch_imported`/`now` to validation. In `stage_account`, after post-existence validation, if imported: check 27 regress (`a.timestamp > ts.last`); assign `acct.timestamp = a.timestamp` (user) and set `ts.last = a.timestamp`; else `acct.timestamp = ts.next(now)`. Remove the `account_is_gated` gate. Watermark = `ts.last`.
- [ ] `create_transfers`: compute `batch_imported` from the first transfer; capture `now`. Pass `batch_imported` to `process_transfer`.
- [ ] `process_transfer`: add the id_already_failed check (after existence, before post-existence): `if failed_ids.contains(&t.id) || is_failed(..t.id).await? { return Ok(R::IdAlreadyFailed); }`. Compute `imported_ts = batch_imported.then_some(t.timestamp)`. Pass `imported_ts` + `ts.peek(now)`/`ts.last` into the stage fns for the 54-57 checks. On accept, assign `timestamp = imported_ts.unwrap_or_else(|| ts.next(now))`; if imported, also `ts` must advance: set via a new `ts.assign_imported(imported_ts)` that sets `last` (regress already validated). For non-imported, `ts.next(now)` advances.
- [ ] stage_regular / stage_pending / stage_resolution: accept `imported_ts: Option<u64>` + `last_ts: u64`; when `Some(ts)`, after loading accounts (resolution: after the 43-53 checks), run 54 regress (`ts > last_ts` → `ImportedEventTimestampMustNotRegress`), 55 (`ts > debit.timestamp`), 56 (`ts > credit.timestamp`), 57 (`t.timeout == 0` → `ImportedEventTimeoutMustBeZero`). (For resolution, debit/credit are the pending's accounts.)
- [ ] Failure tracking: a `failed_ids: HashSet<u128>` accumulated in the chain driver — after each chain's results are finalized, add the id of every result where `transfer_burns_id(r)`. Use it for the in-batch id_already_failed check. At batch end, persist every `failed_ids` id via `batch.put(self.keyspace.failed_key(id), [1u8])`. Adjust the write guard so a batch that only burns ids still commits (`!batch.is_empty()` already covers it once we put the failed keys).
- [ ] Remove the `TransferOp::Gated => NotImplementedYet` arm and the account gate.

> `process_transfer` arg count grows — allow `clippy::too_many_arguments` (already allowed) or bundle into a small `BatchCtx { now, batch_imported }`.

## Task 4: tests (ledger.rs)
- [ ] **id_already_failed (transient burns):** transfer to a missing account → `CreditAccountNotFound`; create the account; retry SAME id → `IdAlreadyFailed` (not retried). A NEW id succeeds.
- [ ] **id_already_failed (terminal burns):** a transfer with `ledger 0` → `LedgerMustNotBeZero`; retry same id → `IdAlreadyFailed`.
- [ ] **exists does not burn:** create a transfer (Created); retry → `Exists` (not id_already_failed).
- [ ] **in-batch burn:** `[bad id=5, then id=5 again]` → `[<code>, IdAlreadyFailed]`.
- [ ] **linked_event_failed burns:** a failed chain member's id, retried later → `IdAlreadyFailed`.
- [ ] **imported account:** an imported batch with user timestamps creates accounts with those exact timestamps; out-of-range / advance(future) / regress / non-imported-in-imported-batch each return the right code.
- [ ] **imported transfer:** imported regular transfer with a valid user ts (> both account timestamps, > watermark, ≤ now) → Created with that ts; postdate-debit / postdate-credit / regress / timeout-nonzero each rejected; `imported_event_not_expected` for an imported event in a non-imported batch.
- [ ] **non-imported still requires ts 0** (regression): a non-imported transfer with ts != 0 → `TimestampMustBeZero` (unchanged).
- [ ] **no NotImplementedYet anywhere:** grep/compile confirms the variant is gone; every flag now produces a real TB result.

Run `cargo test -p bluedb-ledger` + `cargo clippy --workspace --all-targets -- -D warnings`. Commit. Spec-compliance + code-quality review; fix; mark Phase G complete.
