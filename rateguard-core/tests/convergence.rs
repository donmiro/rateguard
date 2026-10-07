// Gossip delivers news late, twice and out of order. Membership only
// converges if the outcome depends on which news arrived, not on when.

use proptest::prelude::*;
use rateguard_core::boundary::PeerId;
use rateguard_core::member::MemberTable;
use rateguard_core::membership::{Membership, check_contract};
use rateguard_proto::{Address, Status, Update};

const LOCAL: u64 = 0;
const MEMBERS: u64 = 6;

fn update() -> impl Strategy<Value = Update> {
    (
        0..MEMBERS,
        0u32..4,
        prop_oneof![
            Just(Status::Alive),
            Just(Status::Suspect),
            Just(Status::Dead)
        ],
    )
        .prop_map(|(member, incarnation, status)| Update {
            member,
            addr: Address::V4([10, 0, 0, member as u8], 7946),
            incarnation,
            status,
        })
}

fn news() -> impl Strategy<Value = (Vec<Update>, Vec<Update>)> {
    prop::collection::vec(update(), 0..40).prop_flat_map(|news| {
        let shuffled = Just(news.clone()).prop_shuffle();
        (Just(news), shuffled)
    })
}

fn heard(news: &[Update]) -> MemberTable {
    let mut table = MemberTable::new(
        PeerId::new(LOCAL),
        Address::V4([10, 0, 0, LOCAL as u8], 7946),
    );
    for &update in news {
        table.apply(update, 0);
    }
    table
}

proptest! {
    // About peers the outcome must match exactly. About itself a node may
    // end up at different incarnations: hearing Alive(3) then Dead(2) takes
    // it to 4, the other way round to 3. Either answers everything it heard,
    // which is all a refutation needs (see the last property).
    #[test]
    fn the_order_of_the_news_does_not_matter((news, shuffled) in news()) {
        let (a, b) = (heard(&news), heard(&shuffled));
        for member in (0..MEMBERS).filter(|&member| member != LOCAL) {
            let peer = PeerId::new(member);
            prop_assert_eq!(a.update_about(peer), b.update_about(peer), "member {}", member);
        }
        prop_assert_eq!(a.peers(), b.peers());
    }

    #[test]
    fn the_freshest_news_about_each_peer_wins(news in prop::collection::vec(update(), 0..40)) {
        let table = heard(&news);
        for member in 1..MEMBERS {
            let freshest = news
                .iter()
                .filter(|update| update.member == member)
                .copied()
                .reduce(|best, update| if update.supersedes(&best) { update } else { best });
            prop_assert_eq!(table.update_about(PeerId::new(member)), freshest);
        }
    }

    #[test]
    fn a_node_always_outbids_every_accusation_it_heard(news in prop::collection::vec(update(), 0..40)) {
        let mut table = heard(&news);
        let own = table.update_about(PeerId::new(LOCAL)).unwrap();
        prop_assert_eq!(own.status, Status::Alive);
        for accusation in news.iter().filter(|update| update.member == LOCAL) {
            prop_assert!(
                own == *accusation || own.supersedes(accusation),
                "{:?} does not answer {:?}", own, accusation
            );
        }
        prop_assert!(check_contract(&table).is_ok());
        table.drain_changes();
    }
}
