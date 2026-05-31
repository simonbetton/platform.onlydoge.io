import type { PrimaryId } from '@onlydoge/shared-kernel';

export function configKeyPrimary(): string {
  return 'primary';
}

export function configKeyBlockHeight(networkId: PrimaryId): string {
  return `block_height_n${networkId}`;
}

export function configKeyNetworksUpdated(): string {
  return 'networks_updated';
}

export function configKeyNewlyAddedAddress(networkId: PrimaryId, addressId: PrimaryId): string {
  return `newly_added_address_n${networkId}_a${addressId}`;
}

export function configKeyIndexerSyncTail(networkId: PrimaryId): string {
  return `indexer_sync_tail_n${networkId}`;
}

export function configKeyIndexerSyncProgress(networkId: PrimaryId): string {
  return `indexer_sync_progress_n${networkId}`;
}

export function configKeyIndexerStage(networkId: PrimaryId): string {
  return `indexer_stage_n${networkId}`;
}

export function configKeyIndexerProcessTail(networkId: PrimaryId): string {
  return `indexer_process_tail_n${networkId}`;
}

export function configKeyIndexerFinalizedTail(networkId: PrimaryId): string {
  return `indexer_finalized_tail_n${networkId}`;
}

export function configKeyIndexerReprocessDepth(networkId: PrimaryId): string {
  return `indexer_reprocess_depth_n${networkId}`;
}

export function configKeyIndexerProcessProgress(networkId: PrimaryId): string {
  return `indexer_process_progress_n${networkId}`;
}

export function configKeyIndexerFactTail(networkId: PrimaryId): string {
  return `indexer_fact_tail_n${networkId}`;
}

export function configKeyIndexerFactProgress(networkId: PrimaryId): string {
  return `indexer_fact_progress_n${networkId}`;
}

export function configKeyDogecoinCurrentStateReady(networkId: PrimaryId): string {
  return `dogecoin_current_state_ready_n${networkId}`;
}

export function configKeyDogecoinHistoryReady(networkId: PrimaryId): string {
  return `dogecoin_history_ready_n${networkId}`;
}

export function configKeyDogecoinAnalyticsFactsReady(networkId: PrimaryId): string {
  return `dogecoin_analytics_facts_ready_n${networkId}`;
}

export function configKeyDogecoinAnalyticsFactsTail(networkId: PrimaryId): string {
  return `dogecoin_analytics_facts_tail_n${networkId}`;
}

export function configKeyDogecoinCurrentStateMaterialization(networkId: PrimaryId): string {
  return `dogecoin_current_state_materialization_n${networkId}`;
}

export function configKeyProjectionBootstrapTail(networkId: PrimaryId): string {
  return `projection_bootstrap_tail_n${networkId}`;
}

export function configKeyProjectionBootstrapTargetTail(networkId: PrimaryId): string {
  return `projection_bootstrap_target_tail_n${networkId}`;
}

export function configKeyProjectionBootstrapPhase(networkId: PrimaryId): string {
  return `projection_bootstrap_phase_n${networkId}`;
}

export function configKeyProjectionBootstrapCursorUtxo(networkId: PrimaryId): string {
  return `projection_bootstrap_cursor_utxo_n${networkId}`;
}

export function configKeyProjectionBootstrapCursorBalance(networkId: PrimaryId): string {
  return `projection_bootstrap_cursor_balance_n${networkId}`;
}

export function configKeyProjectionBootstrapStartedAt(networkId: PrimaryId): string {
  return `projection_bootstrap_started_at_n${networkId}`;
}

export function configKeyIndexerLink(networkId: PrimaryId, addressId: PrimaryId): string {
  return `indexer_link_n${networkId}_a${addressId}`;
}
