import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

#Check that mandatory columns have no empty values
def check_completeness(df, mandatory_columns):
    for col in mandatory_columns:
        if col not in df.columns:
            continue
        
        is_empty = df[col].isna() | (df[col].astype(str).str.strip() == '')
        df.loc[is_empty, 'quality_flag'] = 'FAIL'
        df.loc[is_empty, 'quality_issues'] += f"Kolom '{col}' kosong; "
    
    return df

#Check that values match valid categories
def check_validity(df, valid_values):

    for col, valid_list in valid_values.items():
        if col in df.columns:
            is_not_valid = ~df[col].astype(str).str.strip().isin(valid_list)
            is_not_empty = (df[col].notna()) & (df[col].astype(str).str.strip() != '')
            
            is_invalid = is_not_valid & is_not_empty
            df.loc[is_invalid, 'quality_flag'] = 'FAIL'
            df.loc[is_invalid, 'quality_issues'] += f"Kolom '{col}' nilai tidak valid; "
    
    return df

#Check that data follows correct format (datetime or numeric)
def check_format(df, format_patterns):
    for col, format_config in format_patterns.items():
        if col not in df.columns:
            continue
            
        format_type = format_config.get('type')
        
        if format_type == 'datetime':
            date_format = format_config.get('format', '%d/%m/%Y %H:%M:%S')
            for idx, val in df[col].items():
                if pd.isna(val) or str(val).strip() == '':
                    continue
                try:
                    datetime.strptime(str(val).strip(), date_format)
                except:
                    df.loc[idx, 'quality_flag'] = 'FAIL'
                    df.loc[idx, 'quality_issues'] += f"Kolom '{col}' format tanggal salah; "
        
        elif format_type == 'numeric':
            for idx, val in df[col].items():
                if pd.isna(val) or str(val).strip() == '':
                    continue
                try:
                    float(str(val).strip())
                except:
                    df.loc[idx, 'quality_flag'] = 'FAIL'
                    df.loc[idx, 'quality_issues'] += f"Kolom '{col}' harus berupa angka; "
    return df

#Check that no duplicate records exist
def check_uniqueness(df, unique_keys):
    if not unique_keys:
        return df
    
    is_duplicate = df.duplicated(subset=unique_keys, keep=False)
    df.loc[is_duplicate, 'quality_flag'] = 'FAIL'
    df.loc[is_duplicate, 'quality_issues'] += f"Data duplikat pada kolom {unique_keys}; "
    
    return df

#Standardize column names for consistency across datasets
def standardize_columns(df, column_mapping):
    if not column_mapping:
        return df
    
    df = df.rename(columns=column_mapping)
    return df

# Normalize all column names to lowercase
def normalize_column_names(df):
    df.columns = df.columns.str.lower()
    return df

#Transform date format from DD/MM/YYYY to YYYY-MM-DD
def transform_date_format(df, date_columns):
    for col_config in date_columns:
        col = col_config.get('column')
        input_format = col_config.get('input_format', '%d/%m/%Y %H:%M:%S')
        output_format = col_config.get('output_format', '%Y-%m-%d %H:%M:%S')
        
        if col not in df.columns:
            continue
        try:
            df[col] = pd.to_datetime(df[col], format=input_format, errors='coerce')
            df[col] = df[col].dt.strftime(output_format)
        except Exception:
            pass
    
    return df


def apply_quality_checks(df, dataset_name, quality_rules):
    df = df.copy()
    
    df = normalize_column_names(df)
    
    if 'transform' in quality_rules:
        column_mapping = quality_rules['transform'].get('column_mapping', {})
        if column_mapping:
            lowercase_mapping = {k.lower(): v for k, v in column_mapping.items()}
            df = standardize_columns(df, lowercase_mapping)
    
    df['quality_flag'] = 'PASS'
    df['quality_issues'] = ''
    
    if 'completeness' in quality_rules:
        mandatory_columns = quality_rules['completeness'].get('mandatory_columns', [])
        mandatory_columns_lower = [col.lower() for col in mandatory_columns]
        df = check_completeness(df, mandatory_columns_lower)
    
    if 'validity' in quality_rules:
        valid_values = quality_rules['validity'].get('valid_values', {})
        valid_values_lower = {k.lower(): v for k, v in valid_values.items()}
        df = check_validity(df, valid_values_lower)
    
    if 'format' in quality_rules:
        format_patterns = quality_rules['format'].get('patterns', {})
        format_patterns_lower = {k.lower(): v for k, v in format_patterns.items()}
        df = check_format(df, format_patterns_lower)
    
    if 'uniqueness' in quality_rules:
        unique_keys = quality_rules['uniqueness'].get('unique_keys', [])
        unique_keys_lower = [col.lower() for col in unique_keys]
        df = check_uniqueness(df, unique_keys_lower)
    
    df_silver = df[df['quality_flag'] == 'PASS'].copy()
    df_quarantine = df[df['quality_flag'] == 'FAIL'].copy()
    
    if len(df_silver) > 0 and 'transform' in quality_rules:
        date_columns = quality_rules['transform'].get('date_columns', [])
        if date_columns:
            df_silver = transform_date_format(df_silver, date_columns)
    
    if len(df_silver) > 0:
        df_silver = df_silver.drop(columns=['quality_flag', 'quality_issues'])
    
    if len(df_quarantine) > 0:
        df_quarantine['quarantine_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return df_silver, df_quarantine

# Get quality rules configuration for all datasets
def get_quality_rules_config():
    rules_config = {
        'permohonan_kp': {
            'transform': {
                'column_mapping': {
                    'asal program studi': 'program_studi',
                    'nama mahasiswa': 'nama',
                    'nim mahasiswa': 'nim',
                    'jenis permohonan': 'jenis_layanan'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani'],
                    'jenis_layanan': ['Kerja Praktik', 'Magang']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'permohonan_ta': {
            'transform': {
                'column_mapping': {
                    'program studi mahasiswa': 'program_studi',
                    'nama mahasiswa': 'nama',
                    'nim mahasiswa': 'nim',
                    'jenis permohonan': 'jenis_layanan'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'pendaftaran_semester_antara': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'mbkm_mandiri': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'tidak_alih_jenjang': {
            'transform': {
                'column_mapping': {
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'keterangan_mahasiswa_aktif': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'besaran_ukt': {
            'transform': {
                'column_mapping': {
                    'prodi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'pengajuan_cuti': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim mahasiswa': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'rekomendasi_beasiswa_lomba': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim mahasiswa': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        },
        'pengunduran_diri': {
            'transform': {
                'column_mapping': {
                    'nama mahasiswa': 'nama',
                    'program studi': 'program_studi',
                    'jenis layanan': 'jenis_layanan',
                    'nim': 'nim'
                },
                'normalize_columns': True,
                'date_columns': [
                    {
                        'column': 'timestamp',
                        'input_format': '%d/%m/%Y %H:%M:%S',
                        'output_format': '%Y-%m-%d %H:%M:%S'
                    }
                ]
            },
            'completeness': {
                'mandatory_columns': ['id', 'status', 'timestamp', 'nama', 'nim', 'jenis_layanan', 'program_studi']
            },
            'validity': {
                'valid_values': {
                    'status': ['Pending', 'Sudah Dilayani']
                }
            },
            'format': {
                'patterns': {
                    'timestamp': {'type': 'datetime', 'format': '%d/%m/%Y %H:%M:%S'},
                    'id': {'type': 'numeric'}
                }
            },
            'uniqueness': {
                'unique_keys': ['id']
            }
        }
    }
    
    return rules_config