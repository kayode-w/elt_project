import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay=5):
    """Wait for PostgreSQL to be ready."""
    retries = 0
    while retries < max_retries:
        try:
            results = subprocess.run(
                ["pg_isready", "-h", host, "-p", "5432", "-U", "postgres"],
                check=True,
                capture_output=True,
                text=True)
            if "accepting connections" in results.stdout:
                print("Successfully connected to PostgreSQL.")
                return True          
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to Postgres: {e}")
            retries += 1

            print(f"Retrying in {delay} seconds... ({retries}/{5})")
            time.sleep(delay)
    print("Max retries reached. PostgreSQL is not ready.")
    return False

if not wait_for_postgres(host = "source_postgres"): #If the wait_for_postgres fxn returns false, we exit the script
    print("Source PostgreSQL is not ready. Exiting script.")
    exit(1)

print("Starting data transfer from source to destination PostgreSQL...")
# Here you would typically call your ELT script or function to transfer data

source_config = {
    'db_name': 'source_db', 
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres' # This is the service name defined in docker-compose.yaml for the source database
}

destination_config = {
    'db_name': 'destination_db',
    'user': 'postgres',
    'password': 'secret', 
    'host': 'destination_postgres' # This is the service name defined in docker-compose.yaml for the destination database
}

# create dump command for initializing the source database
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['db_name'],
    '-f', 'data_dump.sql', #This file will contain the dumped data from the source database
    '-w' # This will prevent us from getting prompted for a password
]

#Set the environment variable for the password
# subprocess_env = dict(PGPASSWORD = source_config['password'])

# Run the dump command to create a dump of the source database
def run_command(command, env_pw):
    try:
        result = subprocess.run(
            command,
            env=dict(PGPASSWORD=env_pw),
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✅{' '.join(command)} completed.") #This will print ✅ psql dump completed.
    except subprocess.CalledProcessError as e:
        print(f"❌ {' '.join(command)} failed!")
        print("stdout:", e.stdout)
        print("stderr:", e.stderr)
        exit(1)

run_command(dump_command, source_config['password'])


#Push from source to destination
load_command =[
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['db_name'],
    '-a',  # echo all commands (optional)
    '-f', 'data_dump.sql', #This file will contain the dumped data from the source database
]

# Run the load command to transfer data to the destination database
run_command(load_command, destination_config['password'])

print('Ending elt script...')
































































































































































































































































































































