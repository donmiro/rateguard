#!/bin/sh
# ../kubernetes.yaml on a local Kubernetes, k3d: replicas of a service that
# each offer 600 checks a second on one key, under one limit of 1000 a
# second across the fleet, while the deployment scales.
#
#     ./demo.sh up      # a k3d cluster of its own, the manifest, a watcher
#     ./demo.sh watch   # what the fleet admits, every second
#     ./demo.sh scale   # the watcher, while the fleet goes 3 → 6 → 2 → 4
#     ./demo.sh down    # the cluster gone
#
# Needs k3d, kubectl and openssl. Your kubeconfig and current context are
# left alone: everything here goes through .kubeconfig, next to this file.
set -eu
cd "$(dirname "$0")"

CLUSTER=rateguard-demo
export KUBECONFIG="$PWD/.kubeconfig"

# Never against a cluster other than the demo's.
ours() {
	[ "$(kubectl config current-context 2>/dev/null)" = "k3d-$CLUSTER" ] || {
		echo "no k3d cluster $CLUSTER: run ./demo.sh up" >&2
		exit 1
	}
}

up() {
	k3d cluster create "$CLUSTER" --no-lb --wait \
		--kubeconfig-update-default=false --kubeconfig-switch-context=false
	k3d kubeconfig get "$CLUSTER" >"$KUBECONFIG"
	ours
	# The Secret the manifest expects, made as its comment says.
	secrets=$(mktemp -d)
	openssl rand -base64 32 >"$secrets/gossip-secrets"
	openssl rand 16 >"$secrets/key-salt"
	kubectl create secret generic api-rateguard \
		--from-file="$secrets/gossip-secrets" --from-file="$secrets/key-salt"
	rm -r "$secrets"
	kubectl kustomize --load-restrictor LoadRestrictionsNone . | kubectl apply -f -
	kubectl rollout status deployment/api --timeout=180s
	kubectl rollout status deployment/watch --timeout=180s
}

watch() {
	ours
	kubectl logs -f --tail=-1 deployment/watch
}

# The watcher's lines, with each change of scale between them.
scale() {
	ours
	kubectl scale deployment/api --replicas=3 >/dev/null
	# A fresh watcher, so that its output starts with its header.
	kubectl rollout restart deployment/watch >/dev/null
	kubectl rollout status deployment/watch >/dev/null
	until [ "$(kubectl get pods -l app=watch -o name | wc -l)" -eq 1 ] &&
		[ "$(kubectl get pods -l app=api -o name | wc -l)" -eq 3 ]; do
		sleep 1
	done
	kubectl logs -f deployment/watch &
	logs=$!
	trap 'kill $logs 2>/dev/null' EXIT INT TERM
	for replicas in 6 2 4; do
		sleep 15
		echo "      \$ kubectl scale deployment/api --replicas=$replicas"
		kubectl scale deployment/api --replicas="$replicas" >/dev/null
	done
	sleep 20
}

down() {
	k3d cluster delete "$CLUSTER"
	rm -f "$KUBECONFIG"
}

case "${1:-}" in
up | watch | scale | down) "$1" ;;
*)
	echo "usage: $0 up | watch | scale | down" >&2
	exit 2
	;;
esac
