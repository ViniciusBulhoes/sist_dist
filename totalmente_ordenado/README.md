para rodar siga

minikube start

eval $(minikube docker-env)

docker build -t lamport-app:1.0 .

kubectl apply -f lamport-statefulset.yaml

para ver os logs
kubectl logs lamport-0 // kubectl logs lamport-1 // kubectl logs lamport-2
