create table "User"
(
    id                serial
        primary key,
    uuid              varchar(100)                           not null,
    username          varchar(20)                            not null,
    password          varchar(200)                           not null,
    "estaActivo"      boolean      default true              not null,
    "createdAt"       timestamp(3) default CURRENT_TIMESTAMP not null,
    "updatedAt"       timestamp(3) default CURRENT_TIMESTAMP not null,
    "useInfinityAuth" boolean      default false             not null
);

create unique index "User_uuid_key"
    on "User" (uuid);

create unique index "User_username_key"
    on "User" (username);


create table "Ubigeo"
(
    codigo      varchar(6)                             not null
        primary key,
    region      varchar(100)                           not null,
    provincia   varchar(100)                           not null,
    distrito    varchar(100)                           not null,
    "createdAt" timestamp(3) default CURRENT_TIMESTAMP not null,
    "updatedAt" timestamp(3) default CURRENT_TIMESTAMP not null
);

create unique index "Ubigeo_codigo_key"
    on "Ubigeo" (codigo);


create table "PersonaNatural"
(
    dni              varchar(8)                             not null
        primary key,
    "nombreCompleto" varchar(255)                           not null,
    "createdAt"      timestamp(3) default CURRENT_TIMESTAMP not null,
    "updatedAt"      timestamp(3) default CURRENT_TIMESTAMP not null
);

create table "PersonaJuridica"
(
    ruc                  varchar(11)                            not null
        primary key,
    "razonSocial"        varchar(255)                           not null,
    estado               varchar(100)                           not null,
    "condicionDomicilio" varchar(100)                           not null,
    "tipoVia"            varchar(100),
    "nombreVia"          varchar(100),
    "codigoZona"         varchar(100),
    "tipoZona"           varchar(100),
    numero               varchar(100),
    interior             varchar(100),
    lote                 varchar(100),
    departamento         varchar(100),
    manzana              varchar(100),
    kilometro            varchar(100),
    "createdAt"          timestamp(3) default CURRENT_TIMESTAMP not null,
    "updatedAt"          timestamp(3) default CURRENT_TIMESTAMP not null,
    "codigoUbigeo"       varchar(6)
                                                                references "Ubigeo"
                                                                    on update cascade on delete set null
);