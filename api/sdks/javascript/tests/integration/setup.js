import { execSync } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const DOCKER_COMPOSE_FILE = join(__dirname, 'docker-compose.test.yml');
const TIMEOUT = 60000; // 60 seconds

// Check if we're running inside Docker (via docker-compose)
const isInsideDocker = () => {
  return process.env.OBJSTORE_REST_URL || process.env.OBJSTORE_GRPC_HOST;
};

export async function setupDocker() {
  // If running inside Docker, the server is already available
  if (isInsideDocker()) {
    console.log('Running inside Docker, using existing server...');
    console.log('Environment variables:');
    console.log('  OBJSTORE_REST_URL:', process.env.OBJSTORE_REST_URL);
    console.log('  OBJSTORE_GRPC_HOST:', process.env.OBJSTORE_GRPC_HOST);
    console.log('  OBJSTORE_QUIC_URL:', process.env.OBJSTORE_QUIC_URL);

    // Wait a bit for services to be ready
    await new Promise((resolve) => setTimeout(resolve, 2000));
    return;
  }

  console.log('Starting Docker containers...');

  try {
    // Stop any existing containers
    try {
      execSync(`docker compose -f ${DOCKER_COMPOSE_FILE} down -v`, {
        stdio: 'inherit',
      });
    } catch (error) {
      // Ignore errors if containers don't exist
    }

    // Start containers
    execSync(`docker compose -f ${DOCKER_COMPOSE_FILE} up -d`, {
      stdio: 'inherit',
    });

    // Wait for health check
    console.log('Waiting for services to be healthy...');
    const startTime = Date.now();

    while (Date.now() - startTime < TIMEOUT) {
      try {
        const result = execSync(`docker compose -f ${DOCKER_COMPOSE_FILE} ps --format json`, {
          encoding: 'utf-8',
        });

        const containers = JSON.parse(`[${result.trim().split('\n').join(',')}]`);
        const allHealthy = containers.every(
          (c) => c.Health === 'healthy' || c.State === 'running'
        );

        if (allHealthy) {
          console.log('All services are healthy!');
          // Wait a bit more for services to fully initialize
          await new Promise((resolve) => setTimeout(resolve, 2000));
          return;
        }
      } catch (error) {
        // Continue waiting
      }

      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    throw new Error('Timeout waiting for services to be healthy');
  } catch (error) {
    console.error('Failed to setup Docker:', error);
    throw error;
  }
}

export async function teardownDocker() {
  // If running inside Docker, don't tear down (managed externally)
  if (isInsideDocker()) {
    console.log('Running inside Docker, skipping teardown...');
    return;
  }

  console.log('Stopping Docker containers...');

  try {
    execSync(`docker compose -f ${DOCKER_COMPOSE_FILE} down -v`, {
      stdio: 'inherit',
    });
    console.log('Docker containers stopped');
  } catch (error) {
    console.error('Failed to teardown Docker:', error);
  }
}
