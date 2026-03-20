// ELT Pipeline — Jenkins Declarative CI/CD Pipeline
// Stages: Lint → Test → Build Images → Push to Artifactory → Deploy to K8s → Smoke Test

pipeline {
    agent {
        kubernetes {
            yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: python
    image: python:3.11-slim
    command: ['sleep', '99d']
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
  - name: docker
    image: docker:24-dind
    securityContext:
      privileged: true
    env:
    - name: DOCKER_TLS_CERTDIR
      value: ""
  - name: kubectl
    image: bitnami/kubectl:1.29
    command: ['sleep', '99d']
"""
        }
    }

    environment {
        ARTIFACTORY_URL   = 'artifactory.company.com'
        ARTIFACTORY_REPO  = 'elt-pipeline'
        IMAGE_TAG         = "${env.BUILD_NUMBER}-${env.GIT_COMMIT?.take(8) ?: 'dev'}"
        KUBE_NAMESPACE    = 'elt-pipeline'
        PYTHON_PATH       = '/app'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
        timeout(time: 45, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
                echo "Building branch: ${env.BRANCH_NAME} | Tag: ${IMAGE_TAG}"
            }
        }

        stage('Install Dependencies') {
            steps {
                container('python') {
                    sh '''
                        pip install --quiet --no-cache-dir -r requirements.txt
                        pip install --quiet ruff mypy pytest pytest-cov
                    '''
                }
            }
        }

        stage('Lint & Type Check') {
            parallel {
                stage('Ruff Lint') {
                    steps {
                        container('python') {
                            sh 'ruff check src/ api/ dashboard/ airflow/ tests/'
                        }
                    }
                }
                stage('Ruff Format') {
                    steps {
                        container('python') {
                            sh 'ruff format --check src/ api/ dashboard/'
                        }
                    }
                }
                stage('MyPy Types') {
                    steps {
                        container('python') {
                            sh 'mypy src/ api/ --ignore-missing-imports --no-strict-optional'
                        }
                    }
                }
            }
        }

        stage('Unit Tests') {
            steps {
                container('python') {
                    sh '''
                        pytest tests/unit/ \
                            --junitxml=reports/unit-results.xml \
                            --cov=src \
                            --cov-report=xml:reports/coverage.xml \
                            --cov-report=term-missing \
                            -v
                    '''
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'reports/unit-results.xml'
                }
            }
        }

        stage('Integration Tests') {
            when {
                anyOf {
                    branch 'main'
                    branch 'release/*'
                }
            }
            steps {
                container('python') {
                    sh '''
                        pytest tests/integration/ \
                            --junitxml=reports/integration-results.xml \
                            -v
                    '''
                }
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'reports/integration-results.xml'
                }
            }
        }

        stage('Build Docker Images') {
            parallel {
                stage('Build pipeline-runner') {
                    steps {
                        container('docker') {
                            sh "docker build -t ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/pipeline-runner:${IMAGE_TAG} -f docker/pipeline/Dockerfile ."
                        }
                    }
                }
                stage('Build airflow') {
                    steps {
                        container('docker') {
                            sh "docker build -t ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/airflow:${IMAGE_TAG} -f docker/airflow/Dockerfile ."
                        }
                    }
                }
                stage('Build api') {
                    steps {
                        container('docker') {
                            sh "docker build -t ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/api:${IMAGE_TAG} -f docker/api/Dockerfile ."
                        }
                    }
                }
                stage('Build dashboard') {
                    steps {
                        container('docker') {
                            sh "docker build -t ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/dashboard:${IMAGE_TAG} -f docker/dashboard/Dockerfile ."
                        }
                    }
                }
            }
        }

        stage('Push to JFrog Artifactory') {
            steps {
                container('docker') {
                    withCredentials([usernamePassword(
                        credentialsId: 'artifactory-docker-creds',
                        usernameVariable: 'ART_USER',
                        passwordVariable: 'ART_PASS'
                    )]) {
                        sh "docker login ${ARTIFACTORY_URL} -u \${ART_USER} -p \${ART_PASS}"

                        sh """
                            docker push ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/pipeline-runner:${IMAGE_TAG}
                            docker push ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/airflow:${IMAGE_TAG}
                            docker push ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/api:${IMAGE_TAG}
                            docker push ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/dashboard:${IMAGE_TAG}
                        """

                        // Tag as latest on main branch
                        script {
                            if (env.BRANCH_NAME == 'main') {
                                sh """
                                    for svc in pipeline-runner airflow api dashboard; do
                                        docker tag ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/\$svc:${IMAGE_TAG} \
                                                   ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/\$svc:latest
                                        docker push ${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/\$svc:latest
                                    done
                                """
                            }
                        }
                    }
                }
            }
        }

        stage('Deploy to Kubernetes') {
            when {
                anyOf {
                    branch 'main'
                    branch 'release/*'
                }
            }
            steps {
                container('kubectl') {
                    withCredentials([file(credentialsId: 'k8s-kubeconfig', variable: 'KUBECONFIG')]) {
                        // Apply namespace and config
                        sh "kubectl apply -f k8s/namespace.yaml"
                        sh "kubectl apply -f k8s/secrets/ -n ${KUBE_NAMESPACE} --dry-run=client"
                        // Note: in production, secrets are applied via Vault injector or external-secrets

                        // Rolling update API and Dashboard images
                        sh """
                            kubectl set image deployment/elt-api \
                                api=${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/api:${IMAGE_TAG} \
                                -n ${KUBE_NAMESPACE}

                            kubectl set image deployment/elt-dashboard \
                                dashboard=${ARTIFACTORY_URL}/${ARTIFACTORY_REPO}/dashboard:${IMAGE_TAG} \
                                -n ${KUBE_NAMESPACE}
                        """

                        // Apply all manifests
                        sh "kubectl apply -f k8s/minio/ -n ${KUBE_NAMESPACE}"
                        sh "kubectl apply -f k8s/api/ -n ${KUBE_NAMESPACE}"
                        sh "kubectl apply -f k8s/dashboard/ -n ${KUBE_NAMESPACE}"

                        // Apply Argo workflows
                        sh "kubectl apply -f argo/workflows/ -n ${KUBE_NAMESPACE}"

                        // Wait for rollout
                        sh """
                            kubectl rollout status deployment/elt-api -n ${KUBE_NAMESPACE} --timeout=5m
                            kubectl rollout status deployment/elt-dashboard -n ${KUBE_NAMESPACE} --timeout=5m
                        """
                    }
                }
            }
        }

        stage('Smoke Test') {
            when {
                branch 'main'
            }
            steps {
                container('python') {
                    sh 'python scripts/smoke_test.py --base-url https://api.company.com'
                }
            }
        }

    }

    post {
        always {
            cleanWs()
        }
        success {
            slackSend(
                channel: '#elt-pipeline-ci',
                color: 'good',
                message: ":white_check_mark: *ELT Pipeline BUILD SUCCESS* | `${env.JOB_NAME}` #${env.BUILD_NUMBER} | Tag: `${IMAGE_TAG}` | <${env.BUILD_URL}|View>"
            )
        }
        failure {
            slackSend(
                channel: '#elt-pipeline-ci',
                color: 'danger',
                message: ":x: *ELT Pipeline BUILD FAILED* | `${env.JOB_NAME}` #${env.BUILD_NUMBER} | Branch: `${env.BRANCH_NAME}` | <${env.BUILD_URL}|View>"
            )
        }
    }
}
