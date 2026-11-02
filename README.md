# ReliabilityShop Microservices (App3)

Production-grade Cloud-Native Microservices suite for AWS SRE Platform on Amazon EKS.

## Architecture

- **`frontend`**: SRE Storefront & Dashboard with live metrics, reverse proxying to internal APIs.
- **`services/auth`**: JWT Authentication microservice using `JWT_SECRET` from External Secrets Operator.
- **`services/order`**: Order Management microservice interacting with PostgreSQL, Redis cache, and publishing async events to AWS SQS.
- **`services/inventory`**: Product catalog and stock reservation microservice.
- **`services/payment`**: Asynchronous SQS consumer worker processing payment fulfillment.

## CI/CD Pipeline

Automated by GitHub Actions (`.github/workflows/build-push.yml`):
1. Unit testing with coverage analysis.
2. Build container images with minimal multi-stage Dockerfiles.
3. Push immutable image tags (`git-<sha>` and `git-v1.0.0`) to Amazon ECR via AWS OIDC Role authentication.
