# Архитектура распределенных данных для интернет-магазина "Мобильный мир"

## Оглавление
1. [Введение](#введение)
2. [Архитектурные решения](#архитектурные-решения)
3. [Структуры данных MongoDB](#структуры-данных-mongodb)
4. [Структуры данных Cassandra](#структуры-данных-cassandra)
5. [Стратегии шардирования и партиционирования](#стратегии-шардирования-и-партиционирования)
6. [Настройка чтения с реплик (MongoDB)](#настройка-чтения-с-реплик-mongodb)
7. [Стратегии обеспечения целостности (Cassandra)](#стратегии-обеспечения-целостности-cassandra)
8. [Мониторинг и устранение проблем](#мониторинг-и-устранение-проблем)
9. [Примеры команд и конфигураций](#примеры-команд-и-конфигураций)

---

## Введение

Данный документ объединяет архитектурные решения для распределенной системы хранения данных интернет-магазина "Мобильный мир", включая:

- **MongoDB** - для данных, требующих строгой консистентности (товары, остатки)
- **Cassandra** - для данных с высокой нагрузкой записи и геораспределённостью (заказы, корзины, логи)

**Цель:** Обеспечить высокую производительность, отказоустойчивость и масштабируемость при пиковых нагрузках (50,000+ запросов/сек).

---

## Архитектурные решения

### Выбор СУБД по типам данных

| Сущность | СУБД | Обоснование |
|----------|------|-------------|
| **Товары (Products)** | MongoDB | Строгая консистентность, read-heavy, сложные запросы |
| **Остатки товаров (Stock)** | MongoDB | ACID транзакции, строгая консистентность (предотвращение overselling) |
| **Заказы (Orders)** | Cassandra | Write-heavy (50K/сек), геораспределённость, eventual consistency OK |
| **История заказов** | Cassandra | Time-series, read-heavy, eventual consistency OK |
| **Корзины (Carts)** | Cassandra | Write-heavy, TTL, геораспределённость |
| **Пользовательские сессии** | Cassandra | TTL, write-heavy, геораспределённость |
| **Логи событий** | Cassandra | Time-series, очень высокая нагрузка записи |

### Сравнительная таблица решений

| Критерий | MongoDB | Cassandra |
|----------|---------|-----------|
| **Консистентность** | Строгая (ACID) | Eventual (Tunable) |
| **Масштабирование** | Решардинг (полное перераспределение) | Incremental (без полного решардинга) |
| **Write Performance** | Хорошая | Отличная (оптимизирована) |
| **Read Performance** | Отличная | Хорошая (по partition key) |
| **Геораспределённость** | Поддерживается | Нативно (multi-datacenter) |
| **Транзакции** | Поддерживаются | Ограниченные |

---

## Структуры данных MongoDB

### 1. Коллекция Orders

**Схема документа:**
```javascript
{
  _id: ObjectId("..."),                    // ObjectId
  order_number: "ORD-2024-001234",        // String
  customer_id: ObjectId("..."),            // ObjectId
  geo_zone: "moscow",                      // String: moscow, spb, ekb, kaliningrad
  created_at: ISODate("2024-01-15T10:30:00Z"),  // Date
  updated_at: ISODate("2024-01-15T10:30:00Z"),  // Date
  status: "processing",                    // String: pending, processing, shipped, delivered, cancelled
  items: [                                 // Array
    {
      product_id: ObjectId("..."),         // ObjectId
      product_name: "Смартфон X",         // String
      category: "electronics",             // String
      quantity: 2,                         // Number
      price: 49990.00                      // Decimal
    }
  ],
  total_amount: 105970.00,                 // Decimal
  shipping_address: {                      // Object
    city: "Москва",                        // String
    street: "ул. Ленина",                  // String
    house: "10",                           // String
    apartment: "25",                       // String
    postal_code: "101000"                  // String
  },
  payment_method: "card",                  // String
  payment_status: "paid"                   // String: pending, paid, failed
}
```

**Индексы:**
```javascript
// Шард-ключ (автоматически)
{ geo_zone: 1, _id: "hashed" }

// Поиск заказов пользователя
{ customer_id: 1, created_at: -1 }

// Фильтрация по статусу
{ status: 1, created_at: -1 }

// Поиск по номеру заказа
{ order_number: 1 }
```

### 2. Коллекция Products

**Схема документа:**
```javascript
{
  _id: ObjectId("..."),                    // ObjectId
  sku: "SMT-X-BLK-256",                   // String (unique)
  name: "Смартфон X 256GB Black",         // String
  category: "electronics",                 // String: electronics, audio, appliances, books
  subcategory: "smartphones",             // String
  brand: "BrandX",                         // String
  price: 49990.00,                         // Decimal
  currency: "RUB",                         // String
  description: "Флагманский смартфон...", // String
  specifications: {                        // Object
    color: "black",                        // String
    memory: "256GB",                       // String
    screen_size: "6.7"                     // String
  },
  images: [                                // Array
    "https://cdn.example.com/products/smt-x-1.jpg"
  ],
  stock_by_geo: [                          // Array
    {
      geo_zone: "moscow",                  // String
      quantity: 150,                       // Number
      warehouse: "MSK-WH-01"               // String
    }
  ],
  total_stock: 310,                        // Number
  is_active: true,                         // Boolean
  created_at: ISODate("2024-01-01T00:00:00Z"),  // Date
  updated_at: ISODate("2024-01-15T10:00:00Z"),  // Date
  rating: 4.7,                             // Decimal
  reviews_count: 1523                      // Number
}
```

**Индексы:**
```javascript
// Шард-ключ (автоматически)
{ _id: "hashed" }

// Поиск по категории
{ category: 1, price: 1, is_active: 1 }

// Поиск по бренду
{ brand: 1, category: 1 }

// Уникальный SKU
{ sku: 1 }  // unique: true

// Полнотекстовый поиск
{ name: "text", description: "text", brand: "text" }
```

### 3. Коллекция Carts

**Схема документа:**
```javascript
{
  _id: ObjectId("..."),                    // ObjectId
  user_id: ObjectId("..."),                // ObjectId (null для гостей)
  session_id: "sess_abc123xyz",            // String (для гостей)
  status: "active",                        // String: active, ordered, abandoned
  items: [                                 // Array
    {
      product_id: ObjectId("..."),         // ObjectId
      sku: "SMT-X-BLK-256",               // String
      product_name: "Смартфон X 256GB Black",  // String
      quantity: 1,                         // Number
      price: 49990.00,                     // Decimal
      added_at: ISODate("2024-01-15T10:15:00Z")  // Date
    }
  ],
  total_items: 3,                          // Number
  total_amount: 61970.00,                  // Decimal
  created_at: ISODate("2024-01-15T10:15:00Z"),  // Date
  updated_at: ISODate("2024-01-15T10:20:00Z"),  // Date
  expires_at: ISODate("2024-01-22T10:15:00Z"),  // Date (TTL)
  geo_zone: "moscow"                        // String
}
```

**Индексы:**
```javascript
// Шард-ключ (автоматически)
{ _id: "hashed" }

// Поиск активной корзины пользователя
{ user_id: 1, status: 1 }

// Поиск активной корзины гостя
{ session_id: 1, status: 1 }

// TTL index для автоматической очистки
{ expires_at: 1 }  // expireAfterSeconds: 0
```

---

## Структуры данных Cassandra

### 1. Заказы (Orders)

**Таблица 1: Заказы по пользователю**
```cql
CREATE TABLE orders_by_customer (
    customer_id UUID,
    order_id UUID,
    order_number TEXT,
    created_at TIMESTAMP,
    status TEXT,
    geo_zone TEXT,
    total_amount DECIMAL,
    items LIST<FROZEN<order_item>>,
    shipping_address MAP<TEXT, TEXT>,
    payment_status TEXT,
    PRIMARY KEY ((customer_id), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);
```

**Таблица 2: Заказы по геозоне (логистика)**
```cql
CREATE TABLE orders_by_geo_zone (
    geo_zone TEXT,
    order_date DATE,
    order_id UUID,
    customer_id UUID,
    order_number TEXT,
    status TEXT,
    total_amount DECIMAL,
    shipping_address MAP<TEXT, TEXT>,
    PRIMARY KEY ((geo_zone, order_date), order_id)
) WITH CLUSTERING ORDER BY (order_id ASC);
```

**Таблица 3: Заказы по статусу (мониторинг)**
```cql
CREATE TABLE orders_by_status (
    status TEXT,
    order_date DATE,
    order_id UUID,
    customer_id UUID,
    order_number TEXT,
    geo_zone TEXT,
    total_amount DECIMAL,
    updated_at TIMESTAMP,
    PRIMARY KEY ((status, order_date), updated_at, order_id)
) WITH CLUSTERING ORDER BY (updated_at DESC, order_id ASC);
```

### 2. История заказов

```cql
CREATE TABLE order_history (
    customer_id UUID,
    order_id UUID,
    order_number TEXT,
    created_at TIMESTAMP,
    status TEXT,
    total_amount DECIMAL,
    items_count INT,
    PRIMARY KEY ((customer_id), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);
```

### 3. Корзины (Carts)

**Таблица 1: Корзины пользователей**
```cql
CREATE TABLE carts_by_user (
    user_id UUID,
    cart_id UUID,
    status TEXT,
    items LIST<FROZEN<cart_item>>,
    total_items INT,
    total_amount DECIMAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    expires_at TIMESTAMP,
    PRIMARY KEY ((user_id), cart_id)
) WITH CLUSTERING ORDER BY (cart_id ASC)
   AND default_time_to_live = 604800;  -- 7 дней
```

**Таблица 2: Гостевые корзины**
```cql
CREATE TABLE carts_by_session (
    session_id TEXT,
    cart_id UUID,
    status TEXT,
    items LIST<FROZEN<cart_item>>,
    total_items INT,
    total_amount DECIMAL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    expires_at TIMESTAMP,
    PRIMARY KEY ((session_id), cart_id)
) WITH CLUSTERING ORDER BY (cart_id ASC)
   AND default_time_to_live = 604800;  -- 7 дней
```

### 4. Пользовательские сессии

```cql
CREATE TABLE user_sessions (
    session_id TEXT,
    user_id UUID,
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMP,
    last_activity TIMESTAMP,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY ((session_id))
) WITH default_time_to_live = 3600;  -- 1 час
```

### 5. Логи событий

```cql
CREATE TABLE event_logs (
    event_type TEXT,
    event_date DATE,
    event_time TIMESTAMP,
    event_id UUID,
    user_id UUID,
    session_id TEXT,
    event_data MAP<TEXT, TEXT>,
    PRIMARY KEY ((event_type, event_date), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
   AND default_time_to_live = 2592000;  -- 30 дней
```

---

## Стратегии шардирования и партиционирования

### MongoDB: Shard Keys

| Коллекция | Shard Key | Стратегия | Преимущества | Риски |
|-----------|-----------|-----------|--------------|-------|
| **orders** | `{ geo_zone: 1, _id: "hashed" }` | Compound Hashed | Изоляция по геозонам, равномерное распределение | Scatter-gather при поиске по customer_id |
| **products** | `{ _id: "hashed" }` | Hashed | Равномерное распределение, эффективный UPDATE | Scatter-gather при поиске по категории |
| **carts** | `{ _id: "hashed" }` | Hashed | Универсальность (гости + пользователи), равномерность | Scatter-gather при поиске по user_id/session_id |

**Обоснование:**

**Orders:**
- ✅ Геозона изолирует логистические запросы
- ✅ Хеширование `_id` обеспечивает равномерность внутри геозоны
- ⚠️ Поиск по `customer_id` требует scatter-gather (решение: индекс + кеширование)

**Products:**
- ✅ Равномерное распределение всех товаров
- ✅ Эффективное обновление остатков (targeted query)
- ⚠️ Поиск по категории требует scatter-gather (решение: Redis кеширование)

**Carts:**
- ✅ Работает для гостей (session_id) и пользователей (user_id)
- ✅ Равномерное распределение
- ⚠️ Поиск по user_id/session_id требует scatter-gather (решение: кеширование cart_id в Redis)

### Cassandra: Partition Keys

| Таблица | Partition Key | Clustering Keys | Преимущества | Риски |
|---------|---------------|-----------------|--------------|-------|
| **orders_by_customer** | `customer_id` | `created_at DESC, order_id ASC` | Равномерное распределение (UUID), эффективное чтение истории | Hot partition для активных пользователей (низкий риск) |
| **orders_by_geo_zone** | `(geo_zone, order_date)` | `order_id ASC` | Ограничение размера партиции (по дате), изоляция по геозонам | Неравномерность по геозонам (низкий риск) |
| **carts_by_user** | `user_id` | `cart_id ASC` | Маленькие партиции (1-2 корзины), быстрый доступ | Hot partition для активных пользователей (низкий риск) |
| **event_logs** | `(event_type, event_date)` | `event_time DESC, event_id ASC` | Time-series оптимизация, ограничение размера | Hot partition для популярных событий (низкий риск) |

**Обоснование:**

**Равномерное распределение:**
- UUID обеспечивает равномерное распределение
- Составные partition keys с датой ограничивают размер партиций

**Избегание hot partitions:**
- Разделение по датам предотвращает неограниченный рост
- TTL автоматически очищает старые данные

**Минимизация решардинга:**
- Consistent hashing - при добавлении узлов перераспределяется только часть данных
- Incremental scaling без полного решардинга

### Сравнительная таблица стратегий

| Аспект | MongoDB (Hashed) | MongoDB (Compound) | Cassandra |
|--------|------------------|-------------------|-----------|
| **Равномерность** | ✅ Высокая | ✅ Высокая | ✅ Высокая |
| **Изоляция данных** | ❌ Нет | ✅ Да (по первому полю) | ✅ Да (по partition key) |
| **Решардинг** | ❌ Полное перераспределение | ❌ Полное перераспределение | ✅ Incremental |
| **Hot partitions** | ⚠️ Возможны | ⚠️ Возможны | ⚠️ Возможны (низкий риск) |
| **Scatter-gather** | ⚠️ При поиске без shard key | ⚠️ При поиске без shard key | ✅ Нет (по partition key) |

---

## Настройка чтения с реплик (MongoDB)

### Таблица Read Preference

| Коллекция | Операция | Read Preference | Допустимая задержка | Обоснование |
|-----------|----------|----------------|---------------------|-------------|
| **products** | Поиск по категории | `secondaryPreferred` | < 5 сек | Каталог - read-heavy, небольшая задержка OK |
| **products** | Проверка остатков | `primary` | 0 сек | Критично! Предотвращение overselling |
| **orders** | История заказов | `secondaryPreferred` | < 10 сек | Статические данные после создания |
| **orders** | Статус заказа | `primary` | 0 сек | Критично! Пользователь должен видеть актуальный статус |
| **carts** | Получение корзины | `primary` | 0 сек | Критично! Высокая частота обновлений |

### Обоснование выбора

**Primary (строгая консистентность):**
- Проверка остатков - риск overselling
- Статус заказа - плохой UX при устаревшем статусе
- Корзина - риск потери изменений

**Secondary Preferred (eventual consistency):**
- Каталог товаров - изменения редкие, задержка OK
- История заказов - статические данные
- Аналитика - не требует строгой консистентности

---

## Стратегии обеспечения целостности (Cassandra)

### Таблица стратегий

| Сущность | Стратегия | Read Repair Chance | Anti-Entropy Repair | Hinted Handoff | Обоснование |
|----------|-----------|-------------------|-------------------|----------------|-------------|
| **Заказы** | Read Repair + Anti-Entropy | 20% | Еженедельно | ✅ Включен | Критичные данные, высокая консистентность |
| **История заказов** | Read Repair | 10% | ❌ Нет | ✅ Включен | Средняя критичность, регулярные чтения |
| **Корзины** | Read Repair + Hinted Handoff | 30% | ❌ Нет | ✅ Включен | Критичные данные, очень частые чтения |
| **Пользовательские сессии** | Read Repair | 10% | ❌ Нет | ✅ Включен | Средняя критичность, частые чтения |
| **Логи событий** | Hinted Handoff | 0% | ❌ Нет | ✅ Включен | Низкая критичность, редкие чтения |

### Описание стратегий

**1. Hinted Handoff**
- Временное хранение записей при недоступности узла
- Автоматическое восстановление после восстановления узла
- Минимальный overhead

**2. Read Repair**
- Восстановление консистентности при чтении
- Автоматическое обнаружение и исправление расхождений
- Средний overhead (при обнаружении расхождений)

**3. Anti-Entropy Repair**
- Проактивное восстановление консистентности
- Фоновый процесс с Merkle trees
- Высокий overhead, но гарантирует сильную консистентность

### Consistency Levels

| Сущность | Write Consistency | Read Consistency | Обоснование |
|----------|------------------|------------------|-------------|
| **Заказы** | `QUORUM` | `QUORUM` | Баланс между производительностью и консистентностью |
| **История заказов** | `QUORUM` | `ONE` | Чтение может быть eventual |
| **Корзины** | `QUORUM` | `QUORUM` | Критичные данные |
| **Пользовательские сессии** | `ONE` | `ONE` | Низкая критичность, минимальная latency |
| **Логи событий** | `ONE` | `ONE` | Максимальная производительность записи |

---

## Мониторинг и устранение проблем

### Метрики мониторинга MongoDB

#### Базовые метрики шардов

| Метрика | Описание | Критерий дисбаланса | Порог |
|---------|----------|---------------------|-------|
| **shard_data_size_bytes** | Размер данных на шарде | Разница > 20% от среднего | ⚠️ 20-40%, 🔴 > 40% |
| **shard_chunk_count** | Количество chunks | Разница > 20% от среднего | ⚠️ 20-40%, 🔴 > 40% |
| **shard_document_count** | Количество документов | Неравномерное распределение | ⚠️ 20-40%, 🔴 > 40% |

#### Метрики производительности

| Метрика | Описание | Критерий дисбаланса | Порог |
|---------|----------|---------------------|-------|
| **shard_ops_per_second** | Операции в секунду | OPS > 2.5x среднего | ⚠️ 1.5-2.5x, 🔴 > 2.5x |
| **shard_query_latency_ms** | Время отклика (p95) | p95 на 50%+ выше среднего | ⚠️ 30-50%, 🔴 > 50% |
| **shard_cpu_usage_percent** | Утилизация CPU | CPU > 80% при средней < 50% | ⚠️ 60-80%, 🔴 > 80% |
| **wiredtiger_cache_hit_ratio** | Cache hit ratio | < 80% при норме на других | ⚠️ 80-95%, 🔴 < 80% |

#### Метрики репликации

| Метрика | Описание | Критерий проблемы | Порог |
|---------|----------|-------------------|-------|
| **replication_lag_seconds** | Задержка репликации | > 5 секунд | ⚠️ 1-5 сек, 🔴 > 5 сек |
| **oplog_lag_seconds** | Задержка в oplog | > 5 секунд | ⚠️ 1-5 сек, 🔴 > 5 сек |

### Метрики мониторинга Cassandra

| Метрика | Описание | Критерий проблемы | Порог |
|---------|----------|-------------------|-------|
| **read_latency_p99** | p99 латентность чтения | > 50ms | ⚠️ 20-50ms, 🔴 > 50ms |
| **write_latency_p99** | p99 латентность записи | > 50ms | ⚠️ 20-50ms, 🔴 > 50ms |
| **pending_compactions** | Ожидающие компрессии | > 10 | ⚠️ 5-10, 🔴 > 10 |
| **dropped_mutations** | Потерянные мутации | > 0 | 🔴 Любое значение |
| **repair_status** | Статус repair | Failed | 🔴 Failed |

### Действия при обнаружении проблем

#### MongoDB: Горячие шарды

**Симптомы:**
- OPS на шарде > 2.5x среднего
- CPU > 80% при средней < 50%
- p95 latency > 200ms

**Действия:**

1. **Немедленно (< 1 часа):**
   ```bash
   # Включить агрессивное кеширование в Redis
   # Проверить баланс chunks
   mongosh --host mongos:27017 --eval "
     use config;
     db.chunks.aggregate([
       { \$match: { ns: 'mobile_world_db.products' } },
       { \$group: { _id: '\$shard', count: { \$sum: 1 } } }
     ])
   "
   ```

2. **Краткосрочно (< 24 часов):**
   ```javascript
   // Ручная миграция chunks
   sh.stopBalancer()
   sh.moveChunk("mobile_world_db.products", {_id: ObjectId("...")}, "target_shard")
   sh.startBalancer()
   ```

3. **Среднесрочно (< 1 недели):**
   - Добавить read replicas к горячему шарду
   - Включить zone sharding для изоляции популярных категорий (см. вариант [Zoned Tag Sharding](#вариант-zoned-tag-sharding) ниже)

#### Вариант: Zoned Tag Sharding

**Zoned Tag Sharding** — стратегия устранения горячих шардов за счёт явного привязывания диапазонов шард-ключа к группам шардов через **теги (tags)**. Для «Мобильного мира» это даёт возможность выделить категорию «Электроника» в отдельную зону и распределить её по нескольким шардам, не смешивая с остальными категориями.

**Идея подхода:**

1. **Тегирование шардов** — каждому шарду (или группе шардов) назначается тег, например: `electronics_zone`, `other_zone`.
2. **Зоны (zones)** — в конфигурации кластера создаются зоны: диапазон значений шард-ключа → тег. Балансировщик размещает чанки только на шардах с соответствующим тегом.
3. **Применение к products** — шард-ключ должен включать поле для разделения «горячих» и «холодных» данных (например, категорию). Для коллекции `products` подходит составной ключ `{ category: 1, _id: 1 }` или `{ category: 1, _id: "hashed" }`. Зона `electronics_zone` — диапазон по `category` для «Электроника»; зона `other_zone` — остальные значения `category`.
4. **Распределение внутри зоны** — несколько шардов с одним тегом образуют пул; балансировщик распределяет чанки зоны только между ними. «Электроника» (70% трафика) лежит на 2–3 шардах с тегом `electronics_zone`, остальные категории — на шардах с тегом `other_zone`.

**Схема для кейса «Мобильный мир»:**

| Зона                | Тег                | Шарды           | Назначение                     |
|---------------------|--------------------|-----------------|--------------------------------|
| Электроника         | `electronics_zone` | Shard1, Shard2  | Только категория «Электроника» |
| Остальные категории | `other_zone`       | Shard3–Shard5   | Аудио, техника, книги и т.д.   |

**Итог:** горячая категория изолирована и распределена по нескольким шардам; при росте нагрузки можно добавить шард с тегом `electronics_zone` и расширить только эту зону.

**Когда использовать:** явный перекос по одному измерению (категория, регион); готовность зафиксировать шард-ключ с этим измерением и провести настройку зон (среднесрочно).

**Ограничения:** смена шард-ключа — только через полный решардинг; неравномерность внутри зоны компенсируют кешированием, большим числом шардов в зоне или составным ключом с hashed полем; мониторинг дисбаланса внутри зон по-прежнему нужен.

#### MongoDB: Проблемы с репликацией

**Симптомы:**
- Replication lag > 5 секунд
- Oplog lag > 5 секунд

**Действия:**
```javascript
// Проверить статус репликации
rs.status()

// Проверить задержку
rs.printSlaveReplicationInfo()

// Если secondary отстает:
// 1. Проверить сетевую задержку
// 2. Увеличить ресурсы secondary узла
// 3. Рассмотреть использование read preference = primary для критичных операций
```

#### Cassandra: Проблемы с производительностью

**Симптомы:**
- Read/Write latency > 50ms
- Pending compactions > 10

**Действия:**
```bash
# Проверить статус узлов
nodetool status

# Проверить метрики
nodetool tpstats

# Запустить repair
nodetool repair -pr

# Очистить snapshots (если нужно)
nodetool clearsnapshot
```

#### Cassandra: Проблемы с консистентностью

**Симптомы:**
- Dropped mutations > 0
- Read repair не справляется

**Действия:**
```bash
# Запустить полный repair
nodetool repair -pr

# Проверить статус repair
nodetool repair -pr --status

# Увеличить read_repair_chance для критичных таблиц
ALTER TABLE orders_by_customer WITH read_repair_chance = 0.3;
```

### Диаграмма процесса реагирования

```
Обнаружение проблемы (Alert)
         ↓
Подтверждение (mongostat / nodetool)
         ↓
Идентификация причины:
  - Hot shard? → Кеширование + миграция chunks
  - Replication lag? → Проверка сети + ресурсов
  - Consistency issue? → Repair
         ↓
Применение мер
         ↓
Мониторинг эффекта (15-30 минут)
         ↓
Эскалация если не помогло
```

---

## Примеры команд и конфигураций

### MongoDB: Инициализация шардирования

```javascript
// 1. Включить шардирование для БД
sh.enableSharding("mobile_world_db")

// 2. Создать индексы перед шардированием
use mobile_world_db
db.orders.createIndex({ customer_id: 1, created_at: -1 })
db.orders.createIndex({ status: 1, created_at: -1 })
db.orders.createIndex({ order_number: 1 })

// 3. Настроить шардирование
sh.shardCollection("mobile_world_db.orders", { geo_zone: 1, _id: "hashed" })

// 4. Проверить распределение
sh.status()
```

### MongoDB: Настройка репликации

```javascript
// Инициализация replica set для config servers
rs.initiate({
  _id: "configReplSet",
  configsvr: true,
  members: [
    { _id: 0, host: "configSrv1:27017" },
    { _id: 1, host: "configSrv2:27017" },
    { _id: 2, host: "configSrv3:27017" }
  ]
})

// Инициализация replica set для shard
rs.initiate({
  _id: "shard1ReplSet",
  members: [
    { _id: 0, host: "shard1-1:27018" },
    { _id: 1, host: "shard1-2:27018" },
    { _id: 2, host: "shard1-3:27018" }
  ]
})

// Добавление шарда в кластер
sh.addShard("shard1ReplSet/shard1-1:27018,shard1-2:27018,shard1-3:27018")
```

### MongoDB: Проверка баланса

```javascript
// Распределение chunks по шардам
use config
db.chunks.aggregate([
  { $match: { ns: "mobile_world_db.products" } },
  { $group: { _id: "$shard", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// Статус балансировщика
sh.getBalancerState()
sh.isBalancerRunning()
```

### MongoDB: Настройка Read Preference

```python
# Python (PyMongo)
from pymongo import MongoClient, ReadPreference

# По умолчанию secondaryPreferred
client = MongoClient("mongodb://mongos:27017")

# Для критичных операций - primary
product = db.products.with_options(
    read_preference=ReadPreference.PRIMARY
).find_one({"_id": product_id})
```

### Cassandra: Создание таблиц

```cql
-- Заказы по пользователю
CREATE TABLE orders_by_customer (
    customer_id UUID,
    order_id UUID,
    order_number TEXT,
    created_at TIMESTAMP,
    status TEXT,
    geo_zone TEXT,
    total_amount DECIMAL,
    items LIST<FROZEN<order_item>>,
    PRIMARY KEY ((customer_id), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);

-- Настройка read repair
ALTER TABLE orders_by_customer 
WITH read_repair_chance = 0.2;
```

### Cassandra: Настройка репликации

```cql
-- Создание keyspace с репликацией
CREATE KEYSPACE mobile_world
WITH REPLICATION = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3,
    'datacenter2': 3
};

-- Использование keyspace
USE mobile_world;
```

### Cassandra: Запросы

```cql
-- Вставка заказа
INSERT INTO orders_by_customer (
    customer_id, order_id, order_number, created_at, 
    status, geo_zone, total_amount
) VALUES (
    uuid(), uuid(), 'ORD-2024-001234', toTimestamp(now()),
    'pending', 'moscow', 105970.00
) USING CONSISTENCY QUORUM;

-- Чтение истории заказов
SELECT * FROM orders_by_customer 
WHERE customer_id = ?
ORDER BY created_at DESC
LIMIT 10
USING CONSISTENCY ONE;
```

### Cassandra: Мониторинг и repair

```bash
# Статус кластера
nodetool status

# Метрики узла
nodetool tpstats

# Запуск repair
nodetool repair -pr

# Проверка консистентности
nodetool describecluster
```

---

## Диаграммы и таблицы

### Архитектура кластера MongoDB

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└──────────────────────────┬──────────────────────────────┘
                           │
                ┌──────────▼──────────┐
                │   Redis Cache       │
                │   (Hot Data)        │
                └──────────┬──────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
   │ Mongos1 │        │ Mongos2 │       │ Mongos3 │
   └────┬────┘        └────┬────┘       └────┬────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
   │ Shard 1 │        │ Shard 2 │  ...  │ Shard N │
   │(Replica │        │(Replica │       │(Replica │
   │  Set)   │        │  Set)   │       │  Set)   │
   │ 3 nodes │        │ 3 nodes │       │ 3 nodes │
   └─────────┘        └─────────┘       └─────────┘
```

### Архитектура кластера Cassandra

```
┌─────────────────────────────────────────────────────────┐
│                    Client Applications                   │
└──────────────────────────┬──────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   ┌────▼────┐        ┌────▼────┐       ┌────▼────┐
   │ Node 1  │        │ Node 2  │  ...  │ Node N  │
   │(DC1)    │        │(DC1)    │       │(DC2)    │
   └────┬────┘        └────┬────┘       └────┬────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                ┌──────────▼──────────┐
                │  Consistent Hashing │
                │  (Token Ranges)      │
                └─────────────────────┘
```

### Сравнение стратегий шардирования

| Критерий | MongoDB Hashed | MongoDB Compound | Cassandra |
|----------|----------------|------------------|-----------|
| **Равномерность** | ✅ Высокая | ✅ Высокая | ✅ Высокая |
| **Изоляция** | ❌ Нет | ✅ Да | ✅ Да |
| **Решардинг** | ❌ Полный | ❌ Полный | ✅ Incremental |
| **Hot Partitions** | ⚠️ Возможны | ⚠️ Возможны | ⚠️ Низкий риск |
| **Scatter-Gather** | ⚠️ Да | ⚠️ Да | ✅ Нет |

### Поток данных при создании заказа

```
1. Client → MongoDB (Products)
   └─ Проверка остатков (primary)
   
2. Client → Cassandra (Orders)
   └─ Создание заказа (QUORUM)
   
3. Cassandra → Replication
   └─ QUORUM реплик подтверждают запись
   
4. Client → MongoDB (Products)
   └─ Списание остатков (транзакция)
   
5. Background → Read Repair
   └─ Синхронизация реплик Cassandra
```

---

## Заключение

### Ключевые решения

1. **Гибридная архитектура:**
   - MongoDB для данных с строгой консистентностью (товары, остатки)
   - Cassandra для данных с высокой нагрузкой записи (заказы, корзины, логи)

2. **Стратегии шардирования:**
   - MongoDB: Compound Hashed для orders, Hashed для products/carts
   - Cassandra: UUID-based partition keys с ограничением по датам

3. **Read Preference:**
   - Primary для критичных операций (остатки, статус, корзина)
   - Secondary Preferred для read-heavy операций (каталог, история)

4. **Стратегии целостности:**
   - Read Repair + Anti-Entropy для критичных данных
   - Read Repair для важных данных
   - Hinted Handoff для некритичных данных

### Метрики успеха

| Метрика | Целевое значение |
|---------|------------------|
| **MongoDB: Imbalance ratio** | < 20% |
| **MongoDB: OPS skew** | < 1.5x |
| **MongoDB: p95 latency** | < 100ms |
| **MongoDB: Replication lag** | < 1 сек |
| **Cassandra: Read latency p99** | < 50ms |
| **Cassandra: Write latency p99** | < 50ms |
| **Cassandra: Dropped mutations** | 0 |

### Следующие шаги

1. Внедрение мониторинга (Prometheus + Grafana)
2. Настройка автоматических алертов
3. Планирование миграции на Cassandra
4. Тестирование под нагрузкой (50K+ запросов/сек)
5. Документирование runbook для дежурных
