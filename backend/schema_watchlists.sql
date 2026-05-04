CREATE TABLE IF NOT EXISTS watchlists (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watchlist_members (
    watchlist_id INTEGER REFERENCES watchlists(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    PRIMARY KEY (watchlist_id, symbol)
);
