#!/bin/bash

echo "🚀 Running post-creation setup..."

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres -p 5432 -U admin -d sqldq_test; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

echo "✅ PostgreSQL is ready!"

# Initialize the database
echo "📊 Initializing database with sample data..."
PGPASSWORD=admin psql -h postgres -U admin -d sqldq_test -f /workspace/.devcontainer/init-db.sql

if [ $? -eq 0 ]; then
    echo "✅ Database initialization successful!"
else
    echo "❌ Database initialization failed!"
    exit 1
fi


echo "✨ Post-creation setup complete!"
echo "🧪 You can now run 'make test-postgres' to verify the database setup!"
