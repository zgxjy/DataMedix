/
├── .gitignore
├── requirements.txt
├── app_config.py
├── medical_data_extractor.py
├── utils.py
├── assets/
│   └── icons/
│       └── icon.ico
├── db_profiles/
│   ├── __init__.py
│   ├── base_profile.py
│   └── mimic_iv/
│       ├── __init__.py
│       ├── base_info_modules.py
│       ├── profile.py
│       └── panels/
│           ├── __init__.py
│           ├── chartevents_panel.py
│           ├── diagnosis_panel.py
│           ├── labevents_panel.py
│           ├── medication_panel.py
│           └── procedure_panel.py
├── sql_logic/
│   ├── __init__.py
│   └── sql_builder_special.py
├── tabs/
│   ├── __init__.py
│   ├── tab_combine_base_info.py ✅
│   ├── tab_connection.py  ✅
│   ├── tab_data_dictionary.py✅
│   ├── tab_data_export.py✅
│   ├── tab_data_merge.py✅
│   ├── tab_query_cohort.py✅
│   ├── tab_special_data_master.py✅
│   └── tab_structure.py✅
├── tests/
│   ├── __init__.py
│   ├── test_sql_builder_special.py
│   └── test_utils.py
└── ui_components/
    ├── __init__.py
    ├── base_panel.py
    ├── conditiongroup.py
    ├── event_output_widget.py
    ├── time_window_selector_widget.py
    └── value_aggregation_widget.py