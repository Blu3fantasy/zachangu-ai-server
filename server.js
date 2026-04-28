create table if not exists tapiwa_conversation_memory (
  session_id text primary key,
  memory jsonb not null default '{}',
  updated_at timestamptz default now()
);

create table if not exists tapiwa_operational_memory (
  id bigserial primary key,
  memory_key text unique not null,
  memory_value text not null,
  memory_type text default 'business_fact',
  importance text default 'medium',
  source_type text default 'admin_approved',
  source_ref text,
  active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
