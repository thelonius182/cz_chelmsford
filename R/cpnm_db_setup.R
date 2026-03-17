# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Implement connection to the CPNM database inside an SSH-tunnel
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
wait_for_tunnel <- function(tunnel,
                            t_host,
                            t_port,
                            total_timeout = 10,
                            poll_interval = 0.2) {
  start_time <- Sys.time()
  
  repeat {
    if (!tunnel$is_alive()) {
      err <- tunnel$read_all_error()
      stop(
        "SSH tunnel process exited before becoming ready.\n",
        if (has_value(err)) err else "No SSH error output captured."
      )
    }
    
    con <- suppressWarnings(
      try(
        socketConnection(
          host = t_host,
          port = t_port,
          open = "r",
          timeout = 1
        ),
        silent = TRUE
      )
    )
    
    if (!inherits(con, "try-error")) {
      close(con)
      return(invisible(TRUE))
    }
    
    elapsed <- as.numeric(
      difftime(Sys.time(), start_time, units = "secs")
    )
    
    if (elapsed > total_timeout) {
      stop("Setting up the SSH tunnel failed.")
    }
    
    Sys.sleep(poll_interval)
  }
}

close_tunnel <- function(tunnel) {
  
  if (!is.null(tunnel) && tunnel$is_alive()) {
    tunnel$kill()
    tunnel$wait(timeout = 2000)
  }
  
  invisible(NULL)
}

# prepare tunnel+db-settings ----
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

# create SSH-tunnel ----
tunnel <- processx::process$new(
  command = "ssh",
  args = c(
    "-N",
    "-o", "ExitOnForwardFailure=yes",
    "-o", "ServerAliveInterval=30",
    "-o", "ServerAliveCountMax=3",
    "-i", env_tun_auth,
    "-L", env_tun_map,
    env_tun_usr
  ),
  stdout = "|",
  stderr = "|",
  cleanup = TRUE
)

# wait_for_tunnel ----
wait_for_tunnel(
  tunnel = tunnel,
  t_host = env_db_host,
  t_port = env_db_port
)

# connect to db ----
con <- dbConnect(
  drv = RMariaDB::MariaDB(),
  host = env_db_host,
  port = as.integer(env_db_port),
  user = env_db_user,
  password = env_db_pwd,
  dbname = env_db_name
)
