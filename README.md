# WIP: find_in_batches_with_usefulness

Extraction of find_in_batches_with_usefulness from canvas/config/initializers/active_record.rb:619

Makes find_in_batches better, preserving

- Order
- Group
- Distinct

## WARNING

ar_find_in_batches_with_usefulness is being ported from private code used in Instructure's canvas-lms
product. This port is not yet complete, so there may be missing functionality or bugs still remaining.

Currently supports only cursors in postgres.
