# Batch vs. Streaming Divergence

The batch script recomputes features from the full raw event log, so it sees every event in a window before emitting a final value. The streaming pipeline emits results incrementally as each window closes, which means its values can differ from batch results at intermediate points in time.

The largest divergence comes from late data and window closure. With a 30-second watermark tolerance, events that arrive after the watermark has advanced past their window end are dropped from the windowed aggregates. The batch job still counts those records because it processes the complete static dataset. This is expected and desirable for online features because the model needs low-latency outputs rather than retrospective exactness.

For example, `click_rate` and `avg_dwell_time` will match the batch output once a window is finalized and no late data is still within tolerance. Before that point, the streaming result can be slightly lower or higher depending on the mix of events already observed.

# Late Event Handling

The producer intentionally generates late events between 35 and 90 seconds behind the simulated clock. The Flink job uses a bounded out-of-orderness watermark of exactly 30 seconds, so events in that range are late enough to test the pipeline. Events that still fall within the allowed lateness of a window are incorporated before finalization. Events that arrive too late are routed to the late-event side output and counted in the `late_events_dropped` metric.

The dashboard consumes the `pipeline-metrics` topic and shows both `late_events_dropped` and current watermark lag in near real time. That makes it easy to confirm that the pipeline is advancing watermarks, observing late arrivals, and dropping only those records that are beyond the configured tolerance.
