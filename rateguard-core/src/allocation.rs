//! The allocation layer's arithmetic: one node's share of one hot key's
//! limit, from what it knows of the demand across the cluster.
//!
//! A share is the floor `R·β/N` plus a slice of the rest proportional to
//! the node's part of the demand:
//!
//! ```text
//! share = R·β/N + R·(1 − β)·own / (own + others)
//! ```
//!
//! The floor is carved out of `R` rather than added on top, so with one
//! consistent view the shares of all N nodes add up to exactly `R`, and yet
//! a node with no demand yet can admit its first requests (spec §4.3). A
//! node still learning its peers' demand is held to the floor elsewhere,
//! by the node (see [`node`](crate::node)).

/// What a node knows when it sizes its share of one hot key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct View {
    /// R, the cluster-wide limit for the key, in requests per second.
    pub limit: f64,
    /// N, this node included.
    pub cluster_size: usize,
    /// β, in [0, 1]: the part of the limit split evenly as a floor.
    pub floor_factor: f64,
    /// This node's demand for the key, in attempts per second.
    pub own: f64,
    /// The sum of the demand its peers reported for the key.
    pub others: f64,
}

/// The node's share of the key's limit, in requests per second.
pub fn share(view: View) -> f64 {
    let n = view.cluster_size as f64;
    let even = view.limit / n;
    let total = view.own + view.others;
    if total > 0.0 {
        view.limit * view.floor_factor / n
            + view.limit * (1.0 - view.floor_factor) * (view.own / total)
    } else {
        even
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const R: f64 = 1000.0;

    fn view(own: f64, others: f64) -> View {
        View {
            limit: R,
            cluster_size: 5,
            floor_factor: 0.1,
            own,
            others,
        }
    }

    #[test]
    fn the_share_is_the_floor_plus_a_proportional_slice() {
        // 1000 × 0.1 / 5 = 20, plus 900 × 300 / 1200 = 225.
        assert_eq!(share(view(300.0, 900.0)), 245.0);
    }

    #[test]
    fn a_node_with_no_demand_still_gets_the_floor() {
        assert_eq!(share(view(0.0, 900.0)), 20.0);
    }

    #[test]
    fn a_node_alone_with_demand_gets_all_but_the_others_floors() {
        assert_eq!(share(view(300.0, 0.0)), 920.0);
    }

    #[test]
    fn with_no_demand_anywhere_the_limit_is_split_evenly() {
        assert_eq!(share(view(0.0, 0.0)), 200.0);
    }

    mod properties {
        use super::super::*;
        use proptest::prelude::*;

        // One consistent view: every node knows the demand of all the others.
        fn shares(limit: f64, floor_factor: f64, demand: &[f64]) -> Vec<f64> {
            let total: f64 = demand.iter().sum();
            demand
                .iter()
                .map(|&own| {
                    share(View {
                        limit,
                        cluster_size: demand.len(),
                        floor_factor,
                        own,
                        others: total - own,
                    })
                })
                .collect()
        }

        fn cluster() -> impl Strategy<Value = (f64, f64, Vec<f64>)> {
            (
                1.0..1e6f64,
                0.0..=1.0f64,
                prop::collection::vec(prop_oneof![Just(0.0), 0.0..1e6f64], 1..50),
            )
        }

        proptest! {
            #[test]
            fn shares_of_one_view_add_up_to_the_limit((limit, beta, demand) in cluster()) {
                let sum: f64 = shares(limit, beta, &demand).iter().sum();
                prop_assert!((sum - limit).abs() <= limit * 1e-9, "{sum} for a limit of {limit}");
            }

            #[test]
            fn every_node_gets_at_least_the_floor((limit, beta, demand) in cluster()) {
                let floor = limit * beta / demand.len() as f64;
                for s in shares(limit, beta, &demand) {
                    prop_assert!(s >= floor * (1.0 - 1e-12), "{s} under the floor {floor}");
                }
            }

            #[test]
            fn more_demand_never_means_a_smaller_share((limit, beta, demand) in cluster()) {
                let s = shares(limit, beta, &demand);
                for i in 0..demand.len() {
                    for j in 0..demand.len() {
                        if demand[i] > demand[j] {
                            prop_assert!(s[i] >= s[j]);
                        }
                    }
                }
            }
        }
    }
}
