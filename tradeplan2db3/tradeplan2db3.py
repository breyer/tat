def process_tradeplan(conn, data, trade_condition_ids):
    """
    Process the tradeplan.csv and update TradeTemplates and ScheduleMaster accordingly,
    handling both file structures:
      - With 'OptionType' column -> Update PUT or CALL per row.
      - Without 'OptionType' column -> Update both PUT & CALL per row (legacy approach).
    """

    # 1) Identify if we have a 'Plan' column
    if "Plan" in data.columns:
        data['Plan'] = data['Plan'].str.upper()
    else:
        data['Plan'] = 'P1'  # Default everything to Plan P1

    # 2) Identify if we have an 'OptionType' column
    has_option_type = "OptionType" in data.columns

    # 3) Validate strategies in the CSV
    required_conditions = set()
    for strategy in data['Strategy'].unique():
        strategy_upper = strategy.upper()
        if strategy_upper in ["EMA520", "EMA540", "EMA2040"]:
            required_conditions.add(strategy_upper)
            required_conditions.add(f"{strategy_upper}_INV")
        else:
            print(f"Error: Unsupported Strategy '{strategy}'.")
            conn.close()
            sys.exit(1)

    missing_conditions = required_conditions - set(trade_condition_ids.keys())
    if missing_conditions:
        print("Error: Missing TradeConditionIDs for:", missing_conditions)
        conn.close()
        sys.exit(1)

    # 4) Begin updates
    try:
        conn.execute("BEGIN TRANSACTION")

        # For each row, we either do:
        #   - The new approach (per-row PUT or CALL) if OptionType is present
        #   - The old approach (update both PUT and CALL) if OptionType is missing
        for idx, row in data.iterrows():
            plan = row['Plan']
            hour_minute = row['Hour:Minute']
            ema_strategy = row['Strategy'].upper()

            # Convert or check Premium, Spread, Stop, etc.
            premium = float(row['Premium'])
            spread = str(row['Spread']).replace('-', ',')
            stop_str = str(row['Stop'])
            stop_multiple = (float(stop_str[:-1]) if stop_str.lower().endswith('x')
                             else float(stop_str))
            qty_override = int(row['Qty'])

            # Determine the right condition IDs
            if ema_strategy == "EMA520":
                cond_id_put = trade_condition_ids["EMA520"]["id"]
                cond_name_put = trade_condition_ids["EMA520"]["description"]
                cond_id_call = trade_condition_ids["EMA520_INV"]["id"]
                cond_name_call = trade_condition_ids["EMA520_INV"]["description"]
            elif ema_strategy == "EMA540":
                cond_id_put = trade_condition_ids["EMA540"]["id"]
                cond_name_put = trade_condition_ids["EMA540"]["description"]
                cond_id_call = trade_condition_ids["EMA540_INV"]["id"]
                cond_name_call = trade_condition_ids["EMA540_INV"]["description"]
            elif ema_strategy == "EMA2040":
                cond_id_put = trade_condition_ids["EMA2040"]["id"]
                cond_name_put = trade_condition_ids["EMA2040"]["description"]
                cond_id_call = trade_condition_ids["EMA2040_INV"]["id"]
                cond_name_call = trade_condition_ids["EMA2040_INV"]["description"]
            else:
                print(f"Error: Unknown strategy '{ema_strategy}'.")
                conn.rollback()
                conn.close()
                sys.exit(1)

            # ---------- BRANCHING HERE -----------
            if has_option_type:
                # ================ NEW APPROACH (OptionType present) ================
                option_type = str(row['OptionType']).strip().upper()

                if option_type == 'P':
                    # Process only the PUT template
                    template_name = f"PUT SPREAD ({hour_minute}) {plan}"
                    # 1) Update the PUT template
                    update_put_template(
                        conn,
                        template_name,
                        premium,  # => TargetMax
                        spread,   # => LongWidth
                        stop_multiple
                    )
                    # 2) Update the ScheduleMaster (PUT)
                    update_put_schedule_master(
                        conn,
                        template_name,
                        qty_override,
                        ema_strategy,
                        cond_id_put,
                        cond_name_put,
                        plan
                    )

                elif option_type == 'C':
                    # Process only the CALL template
                    template_name = f"CALL SPREAD ({hour_minute}) {plan}"
                    # 1) Update the CALL template
                    update_call_template(
                        conn,
                        template_name,
                        premium,  # => TargetMaxCall
                        spread,   # => LongWidth
                        stop_multiple
                    )
                    # 2) Update the ScheduleMaster (CALL)
                    update_call_schedule_master(
                        conn,
                        template_name,
                        qty_override,
                        ema_strategy + "_INV",
                        cond_id_call,
                        cond_name_call,
                        plan
                    )
                else:
                    print(f"Error: Invalid OptionType '{option_type}' at row {idx + 1}.")
                    conn.rollback()
                    conn.close()
                    sys.exit(1)

            else:
                # ================ OLD APPROACH (NO OptionType) ================
                #  A) Update PUT spread
                put_template_name = f"PUT SPREAD ({hour_minute}) {plan}"
                update_put_template(
                    conn, 
                    put_template_name,
                    premium, spread, stop_multiple
                )
                update_put_schedule_master(
                    conn,
                    put_template_name,
                    qty_override,
                    ema_strategy,
                    cond_id_put,
                    cond_name_put,
                    plan
                )

                #  B) Update CALL spread
                call_template_name = f"CALL SPREAD ({hour_minute}) {plan}"
                update_call_template(
                    conn,
                    call_template_name,
                    premium, spread, stop_multiple
                )
                update_call_schedule_master(
                    conn,
                    call_template_name,
                    qty_override,
                    ema_strategy + "_INV",
                    cond_id_call,
                    cond_name_call,
                    plan
                )

        # If all rows processed without issues
        conn.commit()
        print("\nTradeTemplates and ScheduleMaster entries updated successfully.")

    except sqlite3.Error as e:
        print(f"Error occurred while processing tradeplan: {str(e)}")
        conn.rollback()
        conn.close()
        sys.exit(1)
