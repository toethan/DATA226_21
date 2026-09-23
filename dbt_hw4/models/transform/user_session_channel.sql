with source as (
    SELECT
		userId,
		sessionId,
		channel
	FROM {{source('raw', 'user_session_channel') }}
	WHERE sessionId IS NOT NULL
)

select * from source
