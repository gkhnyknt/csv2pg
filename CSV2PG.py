import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox, scrolledtext
import pandas as pd
import psycopg2
import os
import glob
import threading
import re

# Kolon ve tablo adlarını temizleme/güvenli hale getirme fonksiyonu
def sanitize_db_identifier(name):
    """
    PostgreSQL tablo/sütun adları için bir ismi güvenli hale getirir.
    Boşlukları ve özel karakterleri alt çizgi ile değiştirir, küçük harfe çevirir.
    """
    name = str(name).strip().lower()
    name = re.sub(r'[\s\-.\(\)]+', '_', name)  # Boşluk, tire, nokta, parantezleri _ yap
    name = re.sub(r'[^\w_]', '', name)       # Alfanümerik olmayanları (alt çizgi hariç) kaldır
    name = re.sub(r'__+', '_', name)         # Çoklu alt çizgileri tek yap
    if not name:
        name = "unnamed_identifier"
    # DÜZELTME: 'name' boşsa IndexError'ı önlemek için kontrol eklendi
    if name and name[0].isdigit():
        name = '_' + name                  # Sayıyla başlıyorsa başına _ ekle
    name = name.strip('_')
    return name[:63] # PostgreSQL için genellikle 63 karakter sınırı vardır

# --- Ana İşlem Fonksiyonu (CSV için) ---
def process_csv_to_postgres(db_config, schema_name, folder_path, status_callback):
    """
    Belirtilen klasördeki CSV dosyalarını okur ve PostgreSQL veritabanına aktarır.
    """
    try:
        status_callback(f"PostgreSQL'e bağlanılıyor: {db_config.get('host')}:{db_config.get('port')}/{db_config.get('dbname')}")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        status_callback("Bağlantı başarılı.")

        safe_schema_name = sanitize_db_identifier(schema_name)
        if not safe_schema_name:
            status_callback("HATA: Geçersiz şema adı üretildi. İşlem durduruldu.")
            if 'conn' in locals() and conn and not conn.closed:
                cur.close()
                conn.close()
            return
        
        if schema_name != safe_schema_name:
            status_callback(f"Bilgi: Şema adı '{schema_name}' -> '{safe_schema_name}' olarak düzenlendi.")

        status_callback(f"Şema '{safe_schema_name}' kontrol ediliyor/oluşturuluyor...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{safe_schema_name}\";")
        conn.commit()
        status_callback(f"Şema '{safe_schema_name}' hazır.")

        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            status_callback("Bilgi: Seçilen klasörde CSV dosyası bulunamadı.")
            if 'conn' in locals() and conn and not conn.closed:
                cur.close()
                conn.close()
            return

        total_files = len(csv_files)
        processed_count = 0
        overall_success = True

        for csv_path in csv_files:
            base_name = os.path.splitext(os.path.basename(csv_path))[0]
            table_name = sanitize_db_identifier(base_name)
            if not table_name: # Sanitize sonrası boş kalırsa
                table_name = f"csv_table_{processed_count}"
                status_callback(f"  UYARI: '{base_name}.csv' için geçerli bir tablo adı üretilemedi, '{table_name}' kullanılıyor.")

            qualified_table_name = f'"{safe_schema_name}"."{table_name}"'
            status_callback(f"İşleniyor ({processed_count + 1}/{total_files}): {base_name}.csv -> Tablo: {qualified_table_name}")

            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                status_callback(f"  HATA: {base_name}.csv okunamadı: {e}")
                overall_success = False
                continue

            if df.empty:
                status_callback(f"  UYARI: {base_name}.csv dosyası boş, atlanıyor.")
                continue

            original_columns = df.columns.tolist()
            safe_columns = []
            seen_cols = set()
            for i, col in enumerate(original_columns):
                s_col = sanitize_db_identifier(col if pd.notna(col) else f"col_{i}")
                if not s_col: s_col = f"unnamed_col_{i}"
                temp_col_name = s_col
                counter = 1
                while temp_col_name in seen_cols:
                    temp_col_name = f"{s_col}_{counter}"
                    counter += 1
                s_col = temp_col_name
                safe_columns.append(s_col)
                seen_cols.add(s_col)

            df.columns = safe_columns
            quoted_safe_columns = [f'"{col}"' for col in safe_columns]

            try:
                status_callback(f"  '{qualified_table_name}' tablosu siliniyor (varsa)...")
                cur.execute(f'DROP TABLE IF EXISTS {qualified_table_name} CASCADE;')

                status_callback(f"  '{qualified_table_name}' tablosu oluşturuluyor...")
                create_sql = f'CREATE TABLE {qualified_table_name} (\n'
                create_sql += ",\n".join([f'{col_name} TEXT' for col_name in quoted_safe_columns])
                create_sql += "\n);"
                cur.execute(create_sql)

                # COPY için header olmadan ve index olmadan geçici CSV'ye yaz
                # Windows'ta dosya yolları ile ilgili sorunları önlemek için geçici dosya adını da sanitize edebiliriz.
                temp_csv_filename = sanitize_db_identifier(f"temp_copy_{table_name}") + ".csv"
                temp_csv_for_copy = os.path.join(folder_path, temp_csv_filename)

                df.to_csv(temp_csv_for_copy, index=False, header=False, encoding='utf-8', quoting=1) # csv.QUOTE_ALL

                status_callback(f"  '{qualified_table_name}' tablosuna veri kopyalanıyor...")
                with open(temp_csv_for_copy, 'r', encoding='utf-8') as f:
                    copy_sql = f"COPY {qualified_table_name} ({', '.join(quoted_safe_columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')"
                    cur.copy_expert(sql=copy_sql, file=f)
                
                conn.commit() # Her başarılı tablo aktarımından sonra commit
                status_callback(f"  '{qualified_table_name}' tablosu başarıyla yüklendi.")
                if os.path.exists(temp_csv_for_copy):
                    os.remove(temp_csv_for_copy)
                processed_count += 1

            except psycopg2.Error as e:
                conn.rollback()
                status_callback(f"  PostgreSQL HATA ({qualified_table_name}): {e}")
                overall_success = False
            except Exception as e:
                conn.rollback()
                status_callback(f"  Genel HATA ({qualified_table_name}): {e}")
                overall_success = False
        
        if overall_success and processed_count == total_files and total_files > 0:
            status_callback(f"Tüm {total_files} CSV dosyası başarıyla işlendi.")
        elif processed_count > 0 :
             status_callback(f"{processed_count}/{total_files} CSV dosyası kısmen veya tamamen işlendi. Detaylar için logları kontrol edin.")
        elif total_files == 0:
            pass # Zaten "CSV dosyası bulunamadı" mesajı verildi.
        else: # total_files > 0 ama processed_count == 0
            status_callback(f"Hiçbir CSV dosyası başarıyla işlenemedi. Detaylar için logları kontrol edin.")


    except psycopg2.OperationalError as e:
        status_callback(f"PostgreSQL Bağlantı HATASI: {e}")
    except Exception as e:
        status_callback(f"Beklenmedik bir HATA oluştu: {e}")
    finally:
        if 'conn' in locals() and conn and not conn.closed:
            cur.close()
            conn.close()
            status_callback("PostgreSQL bağlantısı kapatıldı.")

# --- Modern GUI Sınıfı (CSV için) ---
class CsvToPostgresApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("CSV'den PostgreSQL'e Veri Aktarımı (Modern Arayüz)") # Başlık güncellendi
        self.root.geometry("750x750") 

        # --- Değişkenler ---
        self.host_var = ttk.StringVar(value='localhost')
        self.port_var = ttk.StringVar(value='5432')
        self.dbname_var = ttk.StringVar(value='postgres')
        self.user_var = ttk.StringVar(value='postgres')
        self.password_var = ttk.StringVar(value='postgres') 
        self.schema_var = ttk.StringVar(value='public') 
        self.folder_path_var = ttk.StringVar(value="Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
        self.selected_folder_internal = ""

        # --- Çerçeveler ---
        main_frame = ttk.Frame(self.root, padding="10 10 10 10") 
        main_frame.pack(fill=BOTH, expand=YES)

        db_frame = ttk.LabelFrame(main_frame, text="PostgreSQL Bağlantı Bilgileri", padding="10", bootstyle=INFO) 
        db_frame.pack(fill=X, pady=(0,10)) 

        folder_frame = ttk.LabelFrame(main_frame, text="CSV Dosya Kaynağı", padding="10", bootstyle=INFO) # Başlık güncellendi
        folder_frame.pack(fill=X, pady=10)

        action_frame = ttk.Frame(main_frame, padding="0 10 0 0") 
        action_frame.pack(fill=X)

        log_frame = ttk.LabelFrame(main_frame, text="İşlem Günlüğü", padding="10", bootstyle=INFO)
        log_frame.pack(fill=BOTH, expand=YES, pady=(10,0))

        # --- PostgreSQL Bağlantı Bilgileri ---
        labels = ["Host:", "Port:", "Veritabanı Adı:", "Kullanıcı Adı:", "Şifre:", "Şema Adı:"]
        variables = [self.host_var, self.port_var, self.dbname_var, self.user_var, self.password_var, self.schema_var]
        
        for i, label_text in enumerate(labels):
            ttk.Label(db_frame, text=label_text).grid(row=i, column=0, sticky=W, padx=5, pady=5)
            entry_widget = ttk.Entry(db_frame, textvariable=variables[i], width=50)
            if label_text == "Şifre:":
                entry_widget.config(show="*")
            entry_widget.grid(row=i, column=1, sticky=EW, padx=5, pady=5)
        db_frame.grid_columnconfigure(1, weight=1)

        # YENİ: Bağlantı Test Butonu
        self.test_connection_button = ttk.Button(db_frame, text="Bağlantıyı Test Et", command=self.test_db_connection, bootstyle=OUTLINE + INFO)
        self.test_connection_button.grid(row=len(labels), column=0, columnspan=2, sticky=EW, padx=5, pady=(10,5))


        # --- CSV Klasörü Seçimi ---
        self.folder_display_label = ttk.Label(folder_frame, textvariable=self.folder_path_var, wraplength=600, justify=LEFT)
        self.folder_display_label.pack(side=LEFT, fill=X, expand=YES, padx=(0,10))
        
        self.select_folder_button = ttk.Button(folder_frame, text="Klasör Seç", command=self.select_folder, width=15, bootstyle=INFO)
        self.select_folder_button.pack(side=RIGHT)

        # --- Aktarım Butonu ---
        self.transfer_button = ttk.Button(action_frame, text="Veritabanına Aktar", command=self.start_transfer_thread, bootstyle=SUCCESS)
        self.transfer_button.pack(fill=X, ipady=8, pady=(10,0)) 

        # --- Durum/Log Mesajları ---
        self.status_text_widget = scrolledtext.ScrolledText(
            log_frame, height=10, wrap=WORD, relief="sunken", borderwidth=1, state=DISABLED,
            font=('Consolas', 10) 
        )
        self.status_text_widget.pack(fill=BOTH, expand=YES)
        self.status_text_widget.configure(bg="#292929", fg="#cccccc", insertbackground="#ffffff")


    def select_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.selected_folder_internal = folder_selected
            self.folder_path_var.set(f"Seçilen Klasör: {self.selected_folder_internal}")
            self.log_status(f"Klasör seçildi: {self.selected_folder_internal}")
        else:
            if not self.selected_folder_internal:
                 self.folder_path_var.set("Lütfen CSV dosyalarının bulunduğu klasörü seçin.")
            self.log_status("Klasör seçme işlemi iptal edildi veya yeni klasör seçilmedi.")

    def log_status(self, message):
        self.status_text_widget.configure(state=NORMAL)
        self.status_text_widget.insert(END, message + "\n")
        self.status_text_widget.see(END) 
        self.status_text_widget.configure(state=DISABLED)
        self.root.update_idletasks() 

    def _get_db_config(self):
        """Helper method to get and validate DB config from UI."""
        db_config = {
            'host': self.host_var.get().strip(),
            'port': self.port_var.get().strip(),
            'dbname': self.dbname_var.get().strip(),
            'user': self.user_var.get().strip(),
            'password': self.password_var.get() 
        }
        
        if not db_config['host'] or not db_config['dbname'] or not db_config['user']:
            messagebox.showerror("Eksik Bilgi", "Lütfen Host, Veritabanı Adı ve Kullanıcı Adı alanlarını doldurun.")
            return None
        
        try:
            if db_config['port']:
                 db_config['port'] = int(db_config['port'])
            else: # Port boşsa hata ver
                messagebox.showerror("Hata", "Port numarası boş bırakılamaz.")
                return None
        except ValueError:
            messagebox.showerror("Hata", "Port numarası geçerli bir sayı olmalıdır.")
            return None
        
        return db_config

    def test_db_connection(self):
        """Tests the PostgreSQL database connection with the provided credentials."""
        db_config = self._get_db_config()
        if not db_config:
            return

        self.log_status(f"Bağlantı test ediliyor: {db_config['host']}:{db_config['port']}/{db_config['dbname']}...")
        try:
            # Bağlantı testi için kısa bir timeout belirleyebiliriz.
            conn_test = psycopg2.connect(**db_config, connect_timeout=5) 
            conn_test.close()
            self.log_status("Bağlantı testi BAŞARILI!")
            # Başarı mesaj kutusu kaldırıldı, sadece loga yazılıyor.
        except psycopg2.OperationalError as e:
            self.log_status(f"Bağlantı testi BAŞARISIZ: {e}")
            messagebox.showerror("Bağlantı Testi Başarısız", f"Bağlantı kurulamadı:\n{e}")
        except Exception as e: # Diğer olası psycopg2 hataları veya genel hatalar için
            self.log_status(f"Bağlantı testi sırasında beklenmedik HATA: {e}")
            messagebox.showerror("Bağlantı Testi Hatası", f"Beklenmedik bir hata oluştu:\n{e}")


    def start_transfer_thread(self):
        db_config = self._get_db_config()
        if not db_config:
            return

        schema_name = self.schema_var.get().strip()
        if not schema_name:
            messagebox.showerror("Eksik Bilgi", "Lütfen Şema Adı alanını doldurun.")
            return
        
        # Şifre boşsa ana aktarım için kullanıcıya sor (opsiyonel, testte sormadık)
        if not db_config['password']:
             if not messagebox.askyesno("Şifre Eksik", "PostgreSQL şifresi girmediniz. Aktarıma devam etmek istiyor musunuz? (Bazı sunucular şifresiz bağlantıya izin verebilir)"):
                return

        folder_path = self.selected_folder_internal
        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showerror("Eksik Bilgi", "Lütfen geçerli bir CSV dosyalarının bulunduğu klasörü seçin.")
            return

        self.transfer_button.config(state=DISABLED, text="Aktarılıyor...")
        self.test_connection_button.config(state=DISABLED) # Test butonunu da devre dışı bırak
        self.status_text_widget.configure(state=NORMAL)
        self.status_text_widget.delete('1.0', END) 
        self.status_text_widget.configure(state=DISABLED)
        self.log_status("Aktarım işlemi başlatılıyor...")

        # İşlemi ayrı bir thread'de başlat
        transfer_thread = threading.Thread(
            target=process_csv_to_postgres, # CSV işleme fonksiyonunu çağır
            args=(db_config, schema_name, folder_path, self.log_status_thread_safe), 
            daemon=True 
        )
        transfer_thread.start()
        # Thread bittiğinde butonları aktif etmek için kontrol mekanizması
        self.root.after(100, self.check_thread_status, transfer_thread)

    def check_thread_status(self, thread):
        """Thread'in durumunu kontrol eder ve bittiyse butonları aktif eder."""
        if thread.is_alive():
            self.root.after(100, self.check_thread_status, thread)
        else:
            self.enable_buttons() 
            self.log_status("Aktarım işlemi tamamlandı veya durdu.")


    def log_status_thread_safe(self, message):
        """GUI güncellemelerini ana thread üzerinden yapmak için 'after' kullanır."""
        self.root.after(0, self.log_status, message)

    def enable_buttons(self): 
        """Aktarım ve Test butonlarını tekrar aktif hale getirir."""
        self.transfer_button.config(state=NORMAL, text="Veritabanına Aktar")
        self.test_connection_button.config(state=NORMAL)


if __name__ == "__main__":
    main_root = ttk.Window(themename="darkly") 
    app = CsvToPostgresApp(main_root) # Sınıf adını CsvToPostgresApp olarak güncelledik
    main_root.mainloop()
