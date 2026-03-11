-- iOS sms.db schema (simplified for green2blue testing)
-- Based on iOS 17.x Messages database structure

CREATE TABLE IF NOT EXISTS _SqliteDatabaseProperties (
    key TEXT,
    value TEXT,
    UNIQUE(key)
);

INSERT OR IGNORE INTO _SqliteDatabaseProperties (key, value) VALUES ('_UniqueIdentifier', '00000000-0000-0000-0000-000000000000');
INSERT OR IGNORE INTO _SqliteDatabaseProperties (key, value) VALUES ('_DatabaseVersion', '1');

CREATE TABLE IF NOT EXISTS handle (
    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL,
    country TEXT DEFAULT 'us',
    service TEXT DEFAULT 'SMS',
    uncanonicalized_id TEXT
);

CREATE TABLE IF NOT EXISTS chat (
    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
    guid TEXT UNIQUE NOT NULL,
    style INTEGER DEFAULT 45,
    state INTEGER DEFAULT 3,
    account_id TEXT DEFAULT 'p:0',
    properties BLOB,
    chat_identifier TEXT,
    service_name TEXT DEFAULT 'SMS',
    room_name TEXT,
    account_login TEXT,
    is_archived INTEGER DEFAULT 0,
    last_addressed_handle TEXT,
    display_name TEXT DEFAULT '',
    group_id TEXT,
    is_filtered INTEGER DEFAULT 0,
    successful_query INTEGER DEFAULT 1,
    engram_id TEXT,
    server_change_token TEXT,
    ck_sync_state INTEGER DEFAULT 0,
    original_group_id TEXT,
    last_read_message_timestamp INTEGER DEFAULT 0,
    sr_server_change_token TEXT,
    sr_ck_sync_state INTEGER DEFAULT 0,
    cloudkit_record_id TEXT,
    sr_cloudkit_record_id TEXT,
    last_addressed_sim_id TEXT,
    is_blackholed INTEGER DEFAULT 0,
    syndication_date INTEGER DEFAULT 0,
    syndication_type INTEGER DEFAULT 0,
    is_recovered INTEGER DEFAULT 0,
    is_deleting_incoming_messages INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chat_service (
    service TEXT NOT NULL,
    chat INTEGER NOT NULL,
    UNIQUE(service, chat)
);

CREATE TABLE IF NOT EXISTS chat_lookup (
    identifier TEXT NOT NULL,
    domain TEXT NOT NULL,
    chat INTEGER NOT NULL,
    priority INTEGER DEFAULT 0,
    UNIQUE(identifier, domain, chat)
);

CREATE TABLE IF NOT EXISTS message (
    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
    guid TEXT UNIQUE NOT NULL,
    text TEXT,
    replace INTEGER DEFAULT 0,
    service_center TEXT,
    handle_id INTEGER DEFAULT 0,
    subject TEXT,
    country TEXT,
    attributedBody BLOB,
    version INTEGER DEFAULT 1,
    type INTEGER DEFAULT 0,
    service TEXT DEFAULT 'SMS',
    account TEXT DEFAULT 'p:0',
    account_guid TEXT DEFAULT 'p:0',
    error INTEGER DEFAULT 0,
    date INTEGER DEFAULT 0,
    date_read INTEGER DEFAULT 0,
    date_delivered INTEGER DEFAULT 0,
    is_delivered INTEGER DEFAULT 0,
    is_finished INTEGER DEFAULT 1,
    is_emote INTEGER DEFAULT 0,
    is_from_me INTEGER DEFAULT 0,
    is_empty INTEGER DEFAULT 0,
    is_delayed INTEGER DEFAULT 0,
    is_auto_reply INTEGER DEFAULT 0,
    is_prepared INTEGER DEFAULT 0,
    is_read INTEGER DEFAULT 0,
    is_system_message INTEGER DEFAULT 0,
    is_sent INTEGER DEFAULT 0,
    has_dd_results INTEGER DEFAULT 0,
    is_service_message INTEGER DEFAULT 0,
    is_forward INTEGER DEFAULT 0,
    was_downgraded INTEGER DEFAULT 0,
    is_archive INTEGER DEFAULT 0,
    cache_has_attachments INTEGER DEFAULT 0,
    cache_roomnames TEXT,
    was_data_detected INTEGER DEFAULT 0,
    was_deduplicated INTEGER DEFAULT 0,
    is_audio_message INTEGER DEFAULT 0,
    is_played INTEGER DEFAULT 0,
    date_played INTEGER DEFAULT 0,
    item_type INTEGER DEFAULT 0,
    other_handle INTEGER DEFAULT 0,
    group_title TEXT,
    group_action_type INTEGER DEFAULT 0,
    share_status INTEGER DEFAULT 0,
    share_direction INTEGER DEFAULT 0,
    is_expirable INTEGER DEFAULT 0,
    expire_state INTEGER DEFAULT 0,
    message_action_type INTEGER DEFAULT 0,
    message_source INTEGER DEFAULT 0,
    associated_message_guid TEXT,
    associated_message_type INTEGER DEFAULT 0,
    balloon_bundle_id TEXT,
    payload_data BLOB,
    expressive_send_style_id TEXT,
    associated_message_range_location INTEGER DEFAULT 0,
    associated_message_range_length INTEGER DEFAULT 0,
    time_expressive_send_played INTEGER DEFAULT 0,
    message_summary_info BLOB,
    ck_sync_state INTEGER DEFAULT 0,
    ck_record_id TEXT,
    ck_record_change_tag TEXT,
    destination_caller_id TEXT,
    ck_chat_id TEXT,
    sr_ck_sync_state INTEGER DEFAULT 0,
    sr_ck_record_id TEXT,
    sr_ck_record_change_tag TEXT,
    is_corrupt INTEGER DEFAULT 0,
    reply_to_guid TEXT,
    date_recovered INTEGER DEFAULT 0,
    sort_id INTEGER DEFAULT 0,
    is_spam INTEGER DEFAULT 0,
    has_unseen_mention INTEGER DEFAULT 0,
    thread_originator_guid TEXT,
    thread_originator_part TEXT,
    syndication_ranges BLOB,
    was_delivered_quietly INTEGER DEFAULT 0,
    did_notify_recipient INTEGER DEFAULT 0,
    synced_syndication_ranges BLOB,
    date_retracted INTEGER DEFAULT 0,
    date_edited INTEGER DEFAULT 0,
    was_detonated INTEGER DEFAULT 0,
    part_count INTEGER DEFAULT 1,
    is_stewie INTEGER DEFAULT 0,
    is_kt_verified INTEGER DEFAULT 0,
    is_sos INTEGER DEFAULT 0,
    is_critical INTEGER DEFAULT 0,
    bia_reference_id TEXT,
    fallback_hash TEXT
);

CREATE TABLE IF NOT EXISTS attachment (
    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
    guid TEXT UNIQUE NOT NULL,
    created_date INTEGER DEFAULT 0,
    start_date INTEGER DEFAULT 0,
    filename TEXT,
    uti TEXT,
    mime_type TEXT,
    transfer_state INTEGER DEFAULT 5,
    is_outgoing INTEGER DEFAULT 0,
    user_info BLOB,
    transfer_name TEXT,
    total_bytes INTEGER DEFAULT 0,
    is_sticker INTEGER DEFAULT 0,
    sticker_user_info BLOB,
    attribution_info BLOB,
    hide_attachment INTEGER DEFAULT 0,
    ck_sync_state INTEGER DEFAULT 0,
    ck_record_id TEXT,
    original_guid TEXT,
    preview_generation_state INTEGER DEFAULT 0,
    sr_ck_sync_state INTEGER DEFAULT 0,
    sr_ck_record_id TEXT,
    is_commsafety_sensitive INTEGER DEFAULT 0,
    emoji_image_short_description TEXT,
    synced_fallback_image BLOB
);

CREATE TABLE IF NOT EXISTS chat_handle_join (
    chat_id INTEGER,
    handle_id INTEGER,
    UNIQUE(chat_id, handle_id)
);

CREATE TABLE IF NOT EXISTS chat_message_join (
    chat_id INTEGER,
    message_id INTEGER,
    message_date INTEGER DEFAULT 0,
    UNIQUE(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS message_attachment_join (
    message_id INTEGER,
    attachment_id INTEGER,
    UNIQUE(message_id, attachment_id)
);
