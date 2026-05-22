CREATE TABLE `episode_catlg_keys` (
  `ep_id` char(36)
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    NOT NULL
    COMMENT 'Episode UUID. Primary key; one row per episode.',

  `ep_catkey` varchar(255)
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    DEFAULT NULL
    COMMENT 'Origin: Clock Catalogue on GD, column `key-modelrooster` associated with the episode.',

  PRIMARY KEY (`ep_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='Stores keys from GD-ClockCatalogue per episode.'
;
