CREATE TABLE `members` (
  `id` BIGINT NOT NULL COMMENT '成员ID',
  `owner_name` VARCHAR(255) NOT NULL COMMENT '成员姓名',
  `pinyin` VARCHAR(255) DEFAULT NULL COMMENT '成员姓名拼音',
  `nickname` VARCHAR(255) DEFAULT NULL COMMENT '昵称',
  `birthday` VARCHAR(32) DEFAULT NULL COMMENT '生日',
  `birthplace` VARCHAR(255) DEFAULT NULL COMMENT '出生地',
  `constellation` VARCHAR(64) DEFAULT NULL COMMENT '星座',
  `height` INT DEFAULT NULL COMMENT '身高',
  `blood_type` VARCHAR(32) DEFAULT NULL COMMENT '血型',
  `hobbies` TEXT DEFAULT NULL COMMENT '爱好',
  `specialty` TEXT DEFAULT NULL COMMENT '特长',
  `group_id` BIGINT DEFAULT NULL COMMENT '团体ID',
  `group_name` VARCHAR(128) DEFAULT NULL COMMENT '团体名称',
  `team_id` BIGINT DEFAULT NULL COMMENT '队伍ID',
  `team` VARCHAR(128) DEFAULT NULL COMMENT '队伍名称',
  `period_id` BIGINT DEFAULT NULL COMMENT '期数ID',
  `period_name` VARCHAR(128) DEFAULT NULL COMMENT '期数名称',
  `class` VARCHAR(64) DEFAULT NULL COMMENT '班级',
  `jtime` VARCHAR(32) DEFAULT NULL COMMENT '加入时间',
  `ptime` VARCHAR(32) DEFAULT NULL COMMENT '升格时间',
  `gtime` VARCHAR(32) DEFAULT NULL COMMENT '毕业时间',
  `qtime` VARCHAR(32) DEFAULT NULL COMMENT '退团时间',
  `election_rank` VARCHAR(64) DEFAULT NULL COMMENT '总选排名',
  `note` TEXT DEFAULT NULL COMMENT '备注',
  `account` VARCHAR(255) DEFAULT NULL COMMENT '账号标识',
  `room_id` BIGINT DEFAULT NULL COMMENT '房间ID',
  `live_room_id` BIGINT DEFAULT NULL COMMENT '直播房间ID',
  `server_id` BIGINT NOT NULL COMMENT '房间 server_id',
  `channel_id` BIGINT NOT NULL COMMENT '房间 channel_id',
  `wb_uid` VARCHAR(64) DEFAULT NULL COMMENT '微博UID',
  `wb_name` VARCHAR(255) DEFAULT NULL COMMENT '微博名称',
  `avatar` TEXT DEFAULT NULL COMMENT '头像',
  `full_photo1` TEXT DEFAULT NULL COMMENT '大图1',
  `full_photo2` TEXT DEFAULT NULL COMMENT '大图2',
  `full_photo3` TEXT DEFAULT NULL COMMENT '大图3',
  `full_photo4` TEXT DEFAULT NULL COMMENT '大图4',
  `status` INT DEFAULT NULL COMMENT '成员状态',
  `ctime` BIGINT DEFAULT NULL COMMENT '源数据创建时间戳',
  `utime` BIGINT DEFAULT NULL COMMENT '源数据更新时间戳',
  `is_in_group` TINYINT(1) DEFAULT NULL COMMENT '是否在团',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_members_server_id` (`server_id`),
  UNIQUE KEY `uk_members_channel_id` (`channel_id`),
  KEY `idx_members_owner_name` (`owner_name`),
  KEY `idx_members_group_name` (`group_name`),
  KEY `idx_members_team` (`team`),
  KEY `idx_members_room_id` (`room_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='成员表';


CREATE TABLE `messages` (
  `message_id` VARCHAR(128) NOT NULL COMMENT '消息ID',
  `room_id` BIGINT DEFAULT NULL COMMENT '房间ID',
  `server_id` BIGINT NOT NULL COMMENT '房间 server_id',
  `channel_id` BIGINT NOT NULL COMMENT '房间 channel_id',
  `sender_user_id` BIGINT DEFAULT NULL COMMENT '发送者用户ID',
  `sender_name` VARCHAR(255) DEFAULT NULL COMMENT '发送者昵称',
  `member_name` VARCHAR(255) DEFAULT NULL COMMENT '成员名称快照',
  `sender_role` VARCHAR(16) NOT NULL DEFAULT 'fan' COMMENT '发送者角色(member/fan)',
  `message_type` VARCHAR(64) NOT NULL COMMENT '消息类型',
  `sub_type` VARCHAR(64) DEFAULT NULL COMMENT '消息子类型',
  `text_content` LONGTEXT DEFAULT NULL COMMENT '文本内容',
  `ext_info_json` LONGTEXT DEFAULT NULL COMMENT 'ext_info 原始内容',
  `raw_message_json` LONGTEXT DEFAULT NULL COMMENT '原始消息JSON',
  `message_time` DATETIME NOT NULL COMMENT '消息时间',
  `message_time_ms` BIGINT NOT NULL COMMENT '消息毫秒时间戳',
  `is_deleted` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否删除',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`message_id`),
  KEY `idx_messages_room_time` (`room_id`, `message_time`),
  KEY `idx_messages_room_role_time` (`room_id`, `sender_role`, `message_time_ms`),
  KEY `idx_messages_server_time` (`server_id`, `message_time_ms`),
  KEY `idx_messages_server_role_time` (`server_id`, `sender_role`, `message_time_ms`),
  KEY `idx_messages_channel_time` (`channel_id`, `message_time_ms`),
  KEY `idx_messages_sender_role_time` (`sender_role`, `message_time_ms`),
  KEY `idx_messages_sender_user_id` (`sender_user_id`),
  KEY `idx_messages_message_time_ms` (`message_time_ms`),
  CONSTRAINT `fk_messages_server_id`
    FOREIGN KEY (`server_id`) REFERENCES `members` (`server_id`)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息表';


CREATE TABLE `message_payloads` (
  `message_id` VARCHAR(128) NOT NULL COMMENT '消息ID',
  `media_url` TEXT DEFAULT NULL COMMENT '媒体URL',
  `media_path` TEXT DEFAULT NULL COMMENT '本地媒体路径',
  `media_cover_url` TEXT DEFAULT NULL COMMENT '媒体封面URL',
  `media_duration` BIGINT DEFAULT NULL COMMENT '媒体时长',
  `width` INT DEFAULT NULL COMMENT '宽度',
  `height` INT DEFAULT NULL COMMENT '高度',
  `reply_to_message_id` VARCHAR(128) DEFAULT NULL COMMENT '回复目标消息ID',
  `reply_to_text` LONGTEXT DEFAULT NULL COMMENT '回复目标文本',
  `flip_user_name` VARCHAR(255) DEFAULT NULL COMMENT '翻牌用户名称',
  `flip_question` LONGTEXT DEFAULT NULL COMMENT '翻牌问题',
  `flip_answer` LONGTEXT DEFAULT NULL COMMENT '翻牌回答',
  `ext_json` LONGTEXT DEFAULT NULL COMMENT '扩展JSON',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`message_id`),
  CONSTRAINT `fk_message_payloads_message_id`
    FOREIGN KEY (`message_id`) REFERENCES `messages` (`message_id`)
    ON UPDATE CASCADE
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息扩展表';


CREATE TABLE `crawl_tasks` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '任务ID',
  `channel_id` BIGINT NOT NULL COMMENT '房间 channel_id',
  `server_id` BIGINT NOT NULL COMMENT '房间 server_id',
  `task_type` VARCHAR(32) NOT NULL COMMENT '任务类型',
  `status` VARCHAR(32) NOT NULL COMMENT '任务状态',
  `start_time_ms` BIGINT NOT NULL COMMENT '开始时间戳',
  `end_time_ms` BIGINT NOT NULL COMMENT '结束时间戳',
  `last_message_time_ms` BIGINT DEFAULT NULL COMMENT '最后消息时间戳',
  `error_message` TEXT DEFAULT NULL COMMENT '错误信息',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_crawl_tasks_channel_id` (`channel_id`),
  KEY `idx_crawl_tasks_server_id` (`server_id`),
  KEY `idx_crawl_tasks_status` (`status`),
  KEY `idx_crawl_tasks_last_message_time_ms` (`last_message_time_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='抓取任务表';


CREATE TABLE `crawl_checkpoints` (
  `server_id` BIGINT NOT NULL COMMENT '房间 server_id',
  `channel_id` BIGINT NOT NULL COMMENT '房间 channel_id',
  `last_message_id` VARCHAR(128) DEFAULT NULL COMMENT '最后消息ID',
  `last_message_time_ms` BIGINT DEFAULT NULL COMMENT '最后消息时间戳',
  `last_success_at` DATETIME DEFAULT NULL COMMENT '最后成功时间',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`server_id`, `channel_id`),
  KEY `idx_crawl_checkpoints_last_message_time_ms` (`last_message_time_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='抓取断点表';


CREATE TABLE `crawl_history_checkpoints` (
  `server_id` BIGINT NOT NULL COMMENT '房间 server_id',
  `channel_id` BIGINT NOT NULL COMMENT '房间 channel_id',
  `oldest_covered_message_id` VARCHAR(128) DEFAULT NULL COMMENT '已连续覆盖的最老成员消息ID',
  `oldest_covered_time_ms` BIGINT DEFAULT NULL COMMENT '已连续覆盖的最老时间戳',
  `resume_next_time` BIGINT DEFAULT NULL COMMENT '下次历史续翻优先尝试的 nextTime',
  `target_time_ms` BIGINT DEFAULT NULL COMMENT '本次历史补抓目标时间',
  `status` VARCHAR(32) NOT NULL DEFAULT 'idle' COMMENT '历史补抓状态',
  `cursor_verified` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否验证过 resume_next_time 可复用',
  `last_page_count` INT NOT NULL DEFAULT 0 COMMENT '最近一次补抓已翻页数',
  `last_run_started_at` DATETIME DEFAULT NULL COMMENT '最近一次历史补抓开始时间',
  `last_run_finished_at` DATETIME DEFAULT NULL COMMENT '最近一次历史补抓结束时间',
  `last_error_message` TEXT DEFAULT NULL COMMENT '最近一次错误',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`server_id`, `channel_id`),
  KEY `idx_history_oldest_time` (`oldest_covered_time_ms`),
  KEY `idx_history_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='历史抓取断点表';
