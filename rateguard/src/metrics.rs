//! What `Guard::stats` tells, through the `metrics` facade: published by the
//! background task once a tick, so the request path pays nothing for it.
//! All of it but `most_denied`: labels with key hashes would make a series
//! per key, without bound.
//! Every metric carries `guard`, the address the node advertises: a process
//! may run several.

use rateguard_proto::auth::GossipAuth;

use crate::stats::Stats;

pub(crate) fn publish(stats: &Stats, guard: &str) {
    let label = ("guard", guard.to_owned());
    let flag = |on: bool| if on { 1.0 } else { 0.0 };

    let checks = |decision: &'static str| {
        metrics::counter!(
            "rateguard_checks_total",
            &[label.clone(), ("decision", decision.to_owned())]
        )
    };
    checks("allow").absolute(stats.admitted);
    checks("deny").absolute(stats.denied);
    metrics::counter!("rateguard_overflowed_total", std::slice::from_ref(&label))
        .absolute(stats.overflowed);
    metrics::counter!(
        "rateguard_unknown_class_total",
        std::slice::from_ref(&label)
    )
    .absolute(stats.unknown_class);
    metrics::counter!(
        "rateguard_shadow_denied_total",
        std::slice::from_ref(&label)
    )
    .absolute(stats.shadow_denied);
    metrics::counter!(
        "rateguard_identity_conflicts_total",
        std::slice::from_ref(&label)
    )
    .absolute(stats.identity_conflicts);
    metrics::counter!(
        "rateguard_members_refused_total",
        std::slice::from_ref(&label)
    )
    .absolute(stats.members_refused);
    let rejected = [
        ("unsigned", stats.rejected_datagrams.unsigned),
        ("bad_tag", stats.rejected_datagrams.bad_tag),
        ("malformed", stats.rejected_datagrams.malformed),
        // Records, not datagrams: see `RejectedDatagrams`.
        (
            "mismatched_records",
            stats.rejected_datagrams.mismatched_records,
        ),
    ];
    for (reason, count) in rejected {
        metrics::counter!(
            "rateguard_rejected_datagrams_total",
            &[label.clone(), ("reason", reason.to_owned())]
        )
        .absolute(count);
    }

    let gauges = [
        ("rateguard_cluster_size", stats.cluster_size as f64),
        ("rateguard_counted_size", stats.counted_size as f64),
        ("rateguard_suspects", stats.suspects as f64),
        ("rateguard_tracked_keys", stats.tracked_keys as f64),
        ("rateguard_hot_keys", stats.hot_keys as f64),
        ("rateguard_dry_run", flag(stats.dry_run)),
    ];
    for (name, value) in gauges {
        metrics::gauge!(name, std::slice::from_ref(&label)).set(value);
    }
    let held = [
        ("joining", stats.joining),
        ("learning", stats.learning),
        ("without_quorum", stats.without_quorum),
        ("members_full", stats.members_full),
    ];
    for (reason, on) in held {
        metrics::gauge!(
            "rateguard_held",
            &[label.clone(), ("reason", reason.to_owned())]
        )
        .set(flag(on));
    }
    // One series per mode, 1 for the node's own: a fleet half switched
    // shows as two modes summed over the nodes.
    let modes = [
        ("off", GossipAuth::Off),
        ("transitional", GossipAuth::Transitional),
        ("required", GossipAuth::Required),
    ];
    for (name, mode) in modes {
        metrics::gauge!(
            "rateguard_gossip_auth",
            &[label.clone(), ("mode", name.to_owned())]
        )
        .set(flag(stats.gossip_auth == mode));
    }
}
