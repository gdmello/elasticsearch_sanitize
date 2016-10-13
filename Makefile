TAG=0.1
REGISTRY=dev-docker.points.com:80
REPOSITORY=elasticsearch_sanitize

build:
	docker build -t $(REGISTRY)/$(REPOSITORY):$(TAG) .

push:
	docker push $(REGISTRY)/$(REPOSITORY):$(TAG)
