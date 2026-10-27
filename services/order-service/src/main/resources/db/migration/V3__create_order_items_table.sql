CREATE TABLE order_service.order_items
(
    order_id   UUID NOT NULL REFERENCES order_service.orders (id),
    product_id UUID NOT NULL,
    quantity   INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, product_id)
);
