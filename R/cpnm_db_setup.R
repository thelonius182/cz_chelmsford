
# prepare tunnel/database settings
env_tun_auth <- Sys.getenv("CPNM_TUNNEL_AUTH_UBU")
if (!has_value(env_tun_auth)) stop("Missing (Ubuntu-)authentication for SSH-tunnel to CPNM")
env_tun_map <- Sys.getenv("CPNM_TUNNEL_MAPPING")
if (!has_value(env_tun_map)) stop("Missing local/remote mapping for SSH-tunnel to CPNM")
env_tun_usr <- Sys.getenv("CPNM_TUNNEL_USER")
if (!has_value(env_tun_usr)) stop("Missing user for SSH-tunnel to CPNM")

env_db_user <- Sys.getenv("CPNM_DB_USER")
if (!has_value(env_db_user)) stop("Missing CPNM db-user")
env_db_pwd <- Sys.getenv("CPNM_DB_PWD")
if (!has_value(env_db_pwd)) stop("Missing CPNM db-password")
env_db_name <- Sys.getenv("CPNM_DB_NAME")
if (!has_value(env_db_name)) stop("Missing CPNM db-name")
env_db_host <- Sys.getenv("CPNM_DB_HOST")
if (!has_value(env_db_host)) stop("Missing CPNM db-host")
env_db_port <- Sys.getenv("CPNM_DB_PORT")
if (!has_value(env_db_port)) stop("Missing CPNM db-port")

# create SSH-tunnel
tunnel <- system2(
  "ssh",
  args = c("-f", "-N",         # run in background, no command
           "-i", env_tun_auth,
           "-L", env_tun_map,  # local:remote mapping
           env_tun_usr),
  wait = FALSE
)

wait_for_tunnel(env_db_host, env_db_port)

# Use tunnel to connect to CPNM-database
con <- dbConnect(
  drv = RMariaDB::MariaDB(),
  host = env_db_host,
  port = as.integer(env_db_port),
  user = env_db_user,
  password = env_db_pwd,
  dbname = env_db_name
)
