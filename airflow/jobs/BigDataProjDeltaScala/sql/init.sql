create table if not exists userspo{
    id UUID PRIMARY KEY,
    name VARCHAR not null,
    email VARCHAR UNIQUE NOT NULL
    };