.PHONY: check-deps test-postgres psql start-jupyter postgres-info serve-root init-db

# Check if all dependencies are available
check-deps:
	@echo "🔍 Checking dependencies..."
	@python3 -c "import polars; print('✅ Polars available')" || echo "❌ Polars missing"
	@python3 -c "import psycopg2; print('✅ psycopg2 available')" || echo "❌ psycopg2 missing"
	@python3 -c "import duckdb; print('✅ DuckDB available')" || echo "❌ DuckDB missing"
	@python3 -c "import pyspark; print('✅ PySpark available')" || echo "❌ PySpark missing"
	@which psql >/dev/null && echo "✅ PostgreSQL client (psql) available" || echo "❌  psql missing"
	@echo "✅ Dependency check complete!"

# Database shell with fallback
psql:
	echo "🔗 Connecting to PostgreSQL with psql..."; \
	echo "Use \\q to exit, \\dt to list tables, \\d users to describe users table"; \
	PGPASSWORD=admin psql -h postgres -U admin -d sqldq_test; \

init-db:
	PGPASSWORD=admin psql -h postgres -U admin -d sqldq_test -f .devcontainer/init-db.sql

test-postgres:
	@echo "🧪 Testing PostgreSQL connection..."
	@python3 -c "\
import psycopg2; \
import sys; \
conn = psycopg2.connect(host='postgres', database='sqldq_test', user='admin', password='admin', port=5432); \
print('✅ PostgreSQL connection successful!'); \
cur = conn.cursor(); \
cur.execute('SELECT COUNT(*) FROM users;'); \
count = cur.fetchone()[0]; \
print(f'✅ Found {count} users in database'); \
cur.close(); \
conn.close(); \
"

# Show connection info
postgres-info:
	@echo "🔗 Database Connection Information"
	@echo "================================="
	@echo "🏠 Host: postgres"
	@echo "🗄️  Database: sqldq_test"
	@echo "👤 Username: admin"
	@echo "🔐 Password: admin"
	@echo "🔌 Port: 5432"
	@echo ""
	@echo "📝 Connection string:"
	@echo "   postgresql://admin:admin@postgres:5432/sqldq_test"
	@echo ""
	@echo "🔗 Connect via:"
	@echo "   make psql     (psql if available, Python shell as fallback)"
	@echo "   make db-python (Python interactive shell)"

start-jupyter:
	uv run jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''

serve-root: ## Serve the root directory as HTML in browser
	@echo "🌐 Serving root directory at http://localhost:8001"
	@python -m http.server 8001

.PHONY: check
check: ## Run code quality tools
	@echo "🚀 Checking lock file consistency with 'pyproject.toml'"
	@uv lock --locked
	@echo "🚀 Linting code: Running pre-commit"
	@uv run pre-commit run -a

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run python -m pytest --cov --cov-config=pyproject.toml --cov-report=html -s -v

.PHONY: publish
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run python -m pytest --cov --cov-config=pyproject.toml --cov-report=html -s -v

.PHONY: build
build: clean-build ## Build wheel file
	@echo "🚀 Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@echo "🚀 Removing build artifacts"
	@uv run python -c "import shutil; import os; shutil.rmtree('dist') if os.path.exists('dist') else None"

.PHONY: publish
publish: ## Publish a release to PyPI
	@echo "🚀 Publishing."
	@uvx twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

.PHONY: build-and-publish
build-and-publish: build publish ## Build and publish
