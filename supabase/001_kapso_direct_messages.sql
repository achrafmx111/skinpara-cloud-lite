-- Static migration only. Apply manually after review; never embeds credentials.
create table if not exists public.skinpara_channel_messages (
  id bigint generated always as identity primary key,
  event_key text not null unique,
  channel text not null check (channel = 'kapso_direct'),
  customer_key text not null check (customer_key ~ '^cust_[0-9a-f]{40}$'),
  conversation_key text not null check (conversation_key ~ '^conv_[0-9a-f]{40}$'),
  external_message_id text,
  direction text not null check (direction in ('inbound', 'outbound')),
  role text not null check (role in ('user', 'assistant')),
  content text not null check (char_length(content) between 1 and 12000),
  handoff_required boolean not null default false,
  handoff_reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists skinpara_channel_messages_inbound_external_uidx
  on public.skinpara_channel_messages(channel, external_message_id)
  where direction = 'inbound' and external_message_id is not null;

create index if not exists skinpara_channel_messages_history_idx
  on public.skinpara_channel_messages(conversation_key, created_at asc, id asc);

alter table public.skinpara_channel_messages enable row level security;
revoke all on public.skinpara_channel_messages from anon, authenticated;
grant select, insert, update on public.skinpara_channel_messages to service_role;
grant usage, select on sequence public.skinpara_channel_messages_id_seq to service_role;

comment on table public.skinpara_channel_messages is
  'Durable non-PII message history for the isolated SkinPara kapso_direct channel.';
