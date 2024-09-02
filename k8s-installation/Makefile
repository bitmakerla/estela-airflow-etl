PLATFORM ?= linux/amd64
IMAGE_NAME ?= airflow
TAG ?= latest
REGISTRY ?= "localhost:5001"
RELEASE_NAME ?= "airflow"
NAMESPACE ?= "airflow"
SSH_KEY ?= "~/.ssh/dags_ssh"

.PHONY: build
build:
	cd .. && docker build -f docker-installation/Dockerfile --platform $(PLATFORM) -t $(REGISTRY)/$(IMAGE_NAME):$(TAG) .	

.PHONY: push
push: 
	docker push $(REGISTRY)/$(IMAGE_NAME):$(TAG)

.PHONY: install
install:
	-$(MAKE) create-connections
	-$(MAKE) git-sync-credentials
	-helm install $(RELEASE_NAME) apache-airflow/airflow --namespace $(NAMESPACE) --debug --timeout 10m0s -f values.yaml -f override.yaml

.PHONY: upgrade
upgrade:
	-helm upgrade $(RELEASE_NAME) apache-airflow/airflow --namespace $(NAMESPACE) --debug --timeout 10m0s -f values.yaml -f override.yaml

.PHONY: uninstall
uninstall:
	helm uninstall $(RELEASE_NAME) -n $(NAMESPACE)

.PHONY: create-connections
create-connections:
	kubectl apply -f airflow-connections.yaml -n $(NAMESPACE)

.PHONY: git-sync-credentials
git-sync-credentials:
	kubectl apply -f  -n $(NAMESPACE)
