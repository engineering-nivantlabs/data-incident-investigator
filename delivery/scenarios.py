SCENARIOS = {
 'mfts_late': {'name':'Treasury deadline · MFTS-only recovery','fault':'mfts_timeout','clock':720,'story':'Kafka succeeds; required file delivery misses the deadline. Recover only MFTS and prove Kafka publish count is unchanged.'},
 'success': {'name':'Successful end-to-end distribution','fault':'none','clock':120,'story':'Both channels acknowledge every valid record; downstream refresh verifies consumer readiness.'},
 'timeout_retry': {'name':'Ambiguous Kafka acknowledgment','fault':'kafka_timeout','clock':360,'story':'The broker accepts records but acknowledgment times out. Reconcile keys instead of blindly replaying.'},
 'exhausted_retries': {'name':'Kafka callback failures · retry budget exhausted','fault':'kafka_reject','clock':480,'story':'All permitted delivery attempts fail. Advisor blocks further retries and recommends escalation.'},
 'stalled': {'name':'Processing stalled before publication','fault':'stall','clock':480,'story':'The worker records a stall before any channel write. Advance the virtual clock to see deadline breach.'},
 'optional_mfts': {'name':'Optional MFTS does not create a false breach','fault':'mfts_timeout','clock':720,'optional_mfts':True,'story':'Kafka is required and complete; MFTS is optional. The advisor does not recommend unnecessary recovery.'},
 'quarantine': {'name':'Malformed records quarantined','fault':'none','clock':120,'invalid':True,'story':'Invalid records enter quarantine. Valid records may deliver, but full-file completion remains blocked.'},
 'duplicate': {'name':'Duplicate input and idempotent replay','fault':'none','clock':120,'duplicate':True,'story':'Identical source records are deduplicated; replay skips acknowledged distributions.'},
 'kafka_deferred': {'name':'Required Kafka connection deferred','fault':'none','clock':120,'kafka_enabled':False,'story':'Configuration disables required Kafka. Delivery cannot be reported complete.'}
}
