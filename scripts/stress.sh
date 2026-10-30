#!/usr/bin/env bash
# Bounded functional stress/fuzz, not a throughput or latency benchmark.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
export GOWORK=off GOFLAGS=-mod=readonly
case "${1:-quick}" in
  quick)
    count=1 fuzztime=5s
    ;;
  extended)
    count=10 fuzztime=30s
    ;;
  *) echo 'Usage: bash scripts/stress.sh {quick|extended}' >&2; exit 2 ;;
esac
# Keep this inventory focused on waiter scheduling, cancellation/grant/release
# races and Shared lifecycle operations. Full race/coverage still runs every test
# once, including serial Transition/partial/scale tests and benchmark smoke tests.
# Local model runs coordinate real waiter goroutines; Transition models do not.
concurrent_tests=(
  'RoundRobinFIFOAndNoBarging|LimitsCancellationAndReuse|CancelGrantRaceAndDuplicateRelease|ConcurrentCapacityAndTenantReclamation|GrantedSlotCannotBeStolen'
  'AirlockCancelledTenantRejoinsAtTail|AirlockCancelMiddleRequest|AirlockMultipleSlotsAndChurn|DefaultTenantQueueLimitAndCancellation|DefaultTenantParticipatesInRotation'
  'WeightedAtomicAcquisitionAndProtectedHead|WeightedRoundRobinUsesRequestsRatherThanTokens|WeightedProtectedCancellationConsumesTenantTurn|WeightedGrantWinsLaterCancellation|WeightedCancelGrantRaceAndConcurrentRelease|WeightedConcurrentCapacityAndCleanup'
  'ResizeValidationAndDrain|ResizePreservesProtectedHeadAndFIFO|ResizeRejectsAllOversizedWaiters|ResizeCancellationGrantRace|ResizeZeroPauseResume|ResizeZeroPriorityTurns|ResizeZeroPriorityProtectedHead|ResizeZeroReleaseCancelRace|ResizeZeroBudgetsAndCancellation'
  'PrioritySaturatedSharesAndTenantFIFO|PriorityEmptyGroupsDoNotAccumulateCredit|PriorityProtectedCancellationConsumesBothTurns|PriorityUnselectedCancellationDoesNotConsumeTurn|PriorityProtectedLowCannotBeBypassed|PriorityCancelledTenantRejoinsGroupTail|PriorityGrantWinsLaterCancellation|PriorityGroupBudgetsRecoverAfterGrantAndCancel|PriorityCancelGrantRaceAndReclamation|PriorityMixedWeightConcurrentCapacity|PriorityResizeKeepsProtectedHeadAndQueueBudgets'
  'TryAcquireCannotBypassOrReorderQueuedRequests|TryAcquireReleaseWakesQueuedAcquire|TryAcquireMixedWithWaitingAndConcurrentRelease'
  'LocalDynamicModel|LocalProtectedArrivalAndCancellationModel|LocalQueueRecoveryModel|LocalConcurrentStorm|LocalMaxIntConcurrentRelease'
  'SharedConcurrentWeightedLifecycle|SharedConcurrentQueuedCancellation|SharedConcurrentRenewReleaseUnknownOutcome|SharedConcurrentExpiryRejectsLateRenewal|SharedConcurrentLeaseOperations'
  'SharedQueuePollAndGrant|SharedQueuedCancellationCleanupIndependentContext|SharedRenewCannotResurrectExpiredLease|SharedReleaseInvalidatesBeforeIOAndRetries|SharedReleaseCancelsGateWaitAndInvalidatesInFlightRenew|SharedLeaseGateWaitingIsBounded|SharedAcquireUnknownCleanupRemainsBounded'
  'SharedClockAllowance.*|SharedExpiredResultPreservesCallerDeadline|SharedFoldedDeadlineExpiryReportsCallerDeadline'
)
concurrent_pattern="^Test($(IFS='|'; echo "${concurrent_tests[*]}"))$"
# Always repeat normal workloads. Extended size expansion is a separate run so
# large snapshot fixtures do not get multiplied by ten CPU schedules.
export AIRLOCK_STRESS=0
printf 'Concurrency: count=%s cpu=1,2,4 AIRLOCK_STRESS=0\n' "$count"
go test -race -v -shuffle=on -count="$count" -cpu=1,2,4 -timeout=10m \
  -run "$concurrent_pattern" .
# Exercise budget gate/lifecycle barriers without repeating its timed integration
# load. The full race/coverage lane still runs every budget test once.
go test -race -v -shuffle=on -count="$count" -cpu=1,2,4 -timeout=3m \
  -run '^Test(Return|Close|ExpiredChunk|WeightedColdStart|WaitingWeights|InsufficientFreeCapacity|TryAndAcquireValidation|ConcurrentClose|RealSharedFallback|ResizeZeroPreserves|SharedLeaseInvalidation|TryAcquireRechecks|QueuedAcquireRechecks|CancelHeavyWaiter|RejectNonfinite|AcquireFastPath|NewReports)' ./budget
if [[ "${1:-quick}" == extended ]]; then
  export AIRLOCK_STRESS=1
  echo 'Expanded Local/Shared workload: count=1 cpu=1 AIRLOCK_STRESS=1'
  go test -race -v -shuffle=on -count=1 -cpu=1 -timeout=20m \
    -run '^Test(LocalConcurrentStorm|SharedConcurrent(WeightedLifecycle|QueuedCancellation|RenewReleaseUnknownOutcome|ExpiryRejectsLateRenewal))$' .
  # Serial model once, with all 64 seeds x 250 steps; normal coverage already
  # executes the normal 8 seeds x 128 steps without a CPU/repetition multiplier.
  go test -race -v -count=1 -timeout=3m -run '^TestTransitionModelTraces$' .
fi
# Fuzzing has its own small worker budget; do not multiply it by the race CPU loop.
for target in FuzzLocalDynamicModel FuzzTransitionSnapshot FuzzTransitionCommandSequence FuzzOptimisticWindow; do
  go test -run '^$' -fuzz "^${target}$" -fuzztime="$fuzztime" -fuzzminimizetime=1s -parallel=2 -timeout=3m .
done
if [[ "${1:-quick}" == extended ]]; then
  # GOWORK=off intentionally tests each driver's published core pin, not local core.
  for manifest in "$repo"/driver/*/go.mod; do
    (cd "$(dirname "$manifest")" && go test -race -shuffle=on -count=3 -cpu=1,2,4 -timeout=5m ./...)
  done
fi
