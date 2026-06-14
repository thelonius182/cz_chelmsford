library(googledrive)

# designate project-specific cache
options(gargle_oauth_cache = ".secrets")

# check the value of the option, if you like
gargle::gargle_oauth_cache()

# trigger auth on purpose --> store a token in the specified cache
drive_auth()

# see your token file in the cache, if you like
list.files(".secrets/")

library(googledrive)

# trigger auth on purpose --> store a token in the specified cache
drive_auth(cache = ".secrets")
