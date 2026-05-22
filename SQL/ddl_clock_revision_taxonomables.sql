CREATE TABLE `clock_revision_taxonomables` (
  `taxonomy_id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `taxonomable_type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'default',
  `taxonomable_id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `order` int unsigned NOT NULL DEFAULT '0',
  `label` json DEFAULT NULL,
  KEY `taxonomables_taxonomy_id_taxonomable_type_index` (`taxonomy_id`,`taxonomable_type`),
  KEY `taxonomables_taxonomable_lookup_idx` (`taxonomable_type`,`taxonomable_id`),
  KEY `taxonomables_taxonomable_taxonomy_idx` (`taxonomable_id`,`taxonomable_type`,`taxonomy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
