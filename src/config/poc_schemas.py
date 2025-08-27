"""
PoC-based schema definitions that match the working Colab implementation.

This module provides explicit PyArrow schema definitions based on the 
successful PoC, eliminating dynamic schema discovery issues.
"""

import pyarrow as pa
from typing import Dict


def get_poc_schema() -> pa.Schema:
    """
    Get the exact PyArrow schema from the working PoC.
    
    This schema matches the successful Colab implementation and ensures
    perfect compatibility with Redshift COPY operations.
    """
    return pa.schema([
        pa.field('ID', pa.int64()),
        pa.field('billing_num', pa.string()),
        pa.field('partner_id', pa.int32()),
        pa.field('customer_id', pa.int32()),
        pa.field('carrier_id', pa.int32()),
        pa.field('invoice_number', pa.string()),
        pa.field('invoice_date', pa.timestamp('us')),
        pa.field('ant_parcel_no', pa.string()),
        pa.field('uni_no', pa.string()),
        pa.field('uni_sn', pa.string()),
        pa.field('third_party_tno', pa.string()),
        pa.field('parcel_scan_time', pa.timestamp('us')),
        pa.field('batch_number', pa.string()),
        pa.field('reference_number', pa.string()),
        pa.field('warehouse_id', pa.int16()),
        pa.field('airport_code', pa.string()),
        pa.field('consignee_address', pa.string()),
        pa.field('zone', pa.string()),
        pa.field('zipcode', pa.string()),
        pa.field('driver_id', pa.int32()),
        pa.field('actual_weight', pa.decimal128(10, 3)),
        pa.field('dimensional_weight', pa.decimal128(10, 3)),
        pa.field('weight_uom', pa.string()),
        pa.field('net_price', pa.decimal128(10, 2)),
        pa.field('signature_fee', pa.decimal128(10, 2)),
        pa.field('tax', pa.decimal128(10, 2)),
        pa.field('fuel_surcharge', pa.decimal128(10, 2)),
        pa.field('shipping_fee', pa.decimal128(10, 2)),
        pa.field('charge_currency', pa.string()),
        pa.field('province', pa.string()),
        pa.field('latest_status', pa.string()),
        pa.field('last_status_update_at', pa.timestamp('us')),
        pa.field('is_valid', pa.int16()),
        pa.field('create_at', pa.timestamp('us')),
        pa.field('update_at', pa.timestamp('us')),
        pa.field('price_card_name', pa.string()),
        pa.field('in_warehouse_date', pa.timestamp('us')),
        pa.field('inject_warehouse', pa.string()),
        pa.field('gst', pa.decimal128(10, 2)),
        pa.field('qst', pa.decimal128(10, 2)),
        pa.field('consignee_name', pa.string()),
        pa.field('order_create_time', pa.timestamp('us')),
        pa.field('dimension_uom', pa.string()),
        pa.field('length', pa.decimal128(10, 3)),
        pa.field('width', pa.decimal128(10, 3)),
        pa.field('height', pa.decimal128(10, 3)),
        pa.field('org_zipcode', pa.string()),
        pa.field('payment_code', pa.string()),
        pa.field('invoice_to', pa.string()),
        pa.field('remote_surcharge_fees', pa.decimal128(10, 2)),
        pa.field('gv_order_receive_time', pa.timestamp('us'))
    ])


def get_redshift_ddl() -> str:
    """
    Get the exact Redshift DDL from the working PoC.
    
    This DDL matches the PyArrow schema and ensures perfect compatibility.
    """
    return """
    CREATE TABLE IF NOT EXISTS public.settlement_normal_delivery_detail (
        ID BIGINT,
        billing_num VARCHAR(255),
        partner_id INTEGER,
        customer_id INTEGER,
        carrier_id INTEGER,
        invoice_number VARCHAR(255),
        invoice_date TIMESTAMP,
        ant_parcel_no VARCHAR(32),
        uni_no VARCHAR(32),
        uni_sn VARCHAR(32),
        third_party_tno VARCHAR(255),
        parcel_scan_time TIMESTAMP,
        batch_number VARCHAR(255),
        reference_number VARCHAR(255),
        warehouse_id SMALLINT,
        airport_code VARCHAR(8),
        consignee_address VARCHAR(255),
        zone VARCHAR(32),
        zipcode VARCHAR(60),
        driver_id INTEGER,
        actual_weight DECIMAL(10,3),
        dimensional_weight DECIMAL(10,3),
        weight_uom VARCHAR(8),
        net_price DECIMAL(10,2),
        signature_fee DECIMAL(10,2),
        tax DECIMAL(10,2),
        fuel_surcharge DECIMAL(10,2),
        shipping_fee DECIMAL(10,2),
        charge_currency VARCHAR(8),
        province VARCHAR(128),
        latest_status VARCHAR(45),
        last_status_update_at TIMESTAMP,
        is_valid SMALLINT,
        create_at TIMESTAMP,
        update_at TIMESTAMP,
        price_card_name VARCHAR(255),
        in_warehouse_date TIMESTAMP,
        inject_warehouse VARCHAR(8),
        gst DECIMAL(10,2),
        qst DECIMAL(10,2),
        consignee_name VARCHAR(255),
        order_create_time TIMESTAMP,
        dimension_uom VARCHAR(255),
        length DECIMAL(10,3),
        width DECIMAL(10,3),
        height DECIMAL(10,3),
        org_zipcode VARCHAR(255),
        payment_code VARCHAR(255),
        invoice_to VARCHAR(64),
        remote_surcharge_fees DECIMAL(10,2),
        gv_order_receive_time TIMESTAMP
    )
    DISTKEY(ant_parcel_no)
    SORTKEY(create_at, partner_id);
    """