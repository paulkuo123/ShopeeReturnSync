import pandas as pd
import os
import sys
import hashlib
from glob import glob

# ================== 設定區 ==================
主庫存資料夾 = "主庫存"
退貨資料夾 = "退貨單"

# 欄位名稱（請確認與你的檔案完全一致）
退貨_商品ID欄 = "商品ID"
退貨_規格ID欄 = "規格ID"
退貨_數量欄 = "數量"

主_商品ID欄 = "et_title_product_id"
主_選項ID欄 = "et_title_variation_id"
主_庫存欄 = "et_title_variation_stock"

# 異常補貨警示門檻（單一商品規格退貨數量超過此值會提醒人工確認）
WARNING_THRESHOLD = 20
# ===========================================

# ---- 解析命令列參數 ----
DRY_RUN = "--dry-run" in sys.argv

# ---- 日誌功能 (僅在 Dry-Run 時啟用) ----
if DRY_RUN:
    class Logger(object):
        def __init__(self, filename="dry_run_log.txt"):
            self.terminal = sys.stdout
            self.log = open(filename, "w", encoding="utf-8")

        def write(self, message):
            self.terminal.write(message)
            self.log.write(message)
            self.log.flush()

        def flush(self):
            # 為了相容性需要實作 flush
            self.terminal.flush()
            pass

    sys.stdout = Logger()
    print("=" * 55)
    print("  ⚠️  DRY-RUN 預覽模式（不會產生任何輸出檔案）")
    print(f"  📝 完整紀錄將同步存於: dry_run_log.txt")
    print("=" * 55)
print("=== 蝦皮退貨補庫存工具開始執行 ===\n")

# =============================================
# 1. 讀取所有退貨報表
# =============================================
print("正在讀取所有退貨報表...")
all_returns = pd.DataFrame()
已讀取檔案 = []

退貨檔案列表 = glob(os.path.join(退貨資料夾, "*.xlsx")) + glob(os.path.join(退貨資料夾, "*.xls"))

# ---- 安全檢查：偵測完全相同的檔案（防止重複匯入同一份報表）----
def _file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

hash_map = {}  # hash -> 第一個檔名
for f in 退貨檔案列表:
    檔名 = os.path.basename(f)
    if 檔名.startswith('~$'):
        continue
    fhash = _file_md5(f)
    if fhash in hash_map:
        print(f"❌ 【危險：檔案重複】{檔名} 與 {hash_map[fhash]} 內容完全相同！")
        print(f"   請從退貨單資料夾移除其中一份，否則退貨數量會被重複計算！")
    else:
        hash_map[fhash] = 檔名

for f in 退貨檔案列表:
    檔名 = os.path.basename(f)
    if 檔名.startswith('~$'):
        print(f"⏭️  跳過暫存檔: {檔名}")
        continue
    try:
        df = pd.read_excel(f, engine='openpyxl')
        df["__來源檔案__"] = 檔名  # 記錄來源，方便追蹤

        all_returns = pd.concat([all_returns, df], ignore_index=True)
        已讀取檔案.append(檔名)
        print(f"✅ 已讀取退貨報表: {檔名}  ({len(df)} 筆)")
    except Exception as e:
        print(f"❌ 讀取退貨報表失敗 {檔名}: {str(e)[:120]}")

if all_returns.empty:
    print("\n❌ 沒有讀取到任何退貨資料！")
    print("請確認退貨單資料夾內已有「不加密」的 .xlsx 檔案。")
    sys.exit()

# 確保 ID 欄位是字串且去除空格
all_returns[退貨_商品ID欄] = all_returns[退貨_商品ID欄].astype(str).str.strip()
all_returns[退貨_規格ID欄] = all_returns[退貨_規格ID欄].astype(str).str.strip()

returns_sum = all_returns.groupby([退貨_商品ID欄, 退貨_規格ID欄])[退貨_數量欄].sum().reset_index()
print(f"\n✅ 退貨總共 {len(returns_sum)} 種規格需要補貨")

# ---- 安全檢查：跨檔案彙整後的異常大量補貨警示 ----
異常筆 = returns_sum[returns_sum[退貨_數量欄] > WARNING_THRESHOLD]
if not 異常筆.empty:
    print(f"\n⚠️  【異常大量】以下 {len(異常筆)} 筆退貨數量超過 {WARNING_THRESHOLD} 件，請人工確認是否正確：")
    for _, row in 異常筆.iterrows():
        print(f"   → 商品ID: {row[退貨_商品ID欄]} | 規格ID: {row[退貨_規格ID欄]} | 退貨數量: {int(row[退貨_數量欄])} 件")
    if DRY_RUN:
        print("   （Dry-run 模式下不會輸出檔案，請確認後再正式執行）")
    print()

# =============================================
# 2. 處理主庫存檔案
# =============================================
主庫存檔案列表 = glob(os.path.join(主庫存資料夾, "*.xlsx")) + glob(os.path.join(主庫存資料夾, "*.xls"))

for 主檔路徑 in 主庫存檔案列表:
    檔名 = os.path.basename(主檔路徑)
    if 檔名.startswith('~$') or 檔名.startswith('已補退貨_'):
        continue

    print(f"\n{'[DRY-RUN] ' if DRY_RUN else ''}正在處理主庫存檔案: {檔名}")
    print("-" * 50)

    try:
        main = pd.read_excel(主檔路徑, engine='calamine')
    except Exception:
        try:
            main = pd.read_excel(主檔路徑, engine='openpyxl', data_only=True)
        except Exception as e:
            print(f"❌ 無法讀取主庫存檔案 {檔名}: {str(e)[:100]}")
            continue

    # 檢查必要欄位是否存在
    if 主_商品ID欄 not in main.columns or 主_選項ID欄 not in main.columns or 主_庫存欄 not in main.columns:
        print(f"❌ 檔案 {檔名} 缺少必要欄位，請檢查欄位設定！")
        print(f"   當前欄位: {main.columns.tolist()[:10]}...")
        continue

    # 確保 ID 欄位都是字串格式
    main[主_商品ID欄] = main[主_商品ID欄].astype(str).str.strip()
    main[主_選項ID欄] = main[主_選項ID欄].astype(str).str.strip()

    # 只對純數字商品ID的列進行庫存更新，跳過 metadata 說明列
    is_data_row = main[主_商品ID欄].str.match(r'^\d+$', na=False)

    # 合併退貨數量
    merged = main.merge(
        returns_sum,
        left_on=[主_商品ID欄, 主_選項ID欄],
        right_on=[退貨_商品ID欄, 退貨_規格ID欄],
        how='left'
    )

    # ---- 找不到的退貨商品 ----
    找不到的退貨 = merged[merged[主_庫存欄].isna() & merged[退貨_數量欄].notna() & is_data_row]
    if not 找不到的退貨.empty:
        print(f"⚠️  提醒：以下 {len(找不到的退貨)} 筆退貨商品在主庫存中找不到（可能已下架）：")
        for _, row in 找不到的退貨.iterrows():
            print(f"   → 商品ID: {row[退貨_商品ID欄]} | 規格ID: {row[退貨_規格ID欄]} | 退貨數量: {int(row[退貨_數量欄])}")
        print()

    # ---- 計算新庫存 ----
    current_stock = pd.to_numeric(merged.loc[is_data_row, 主_庫存欄], errors='coerce').fillna(0).astype(int)
    return_qty = merged.loc[is_data_row, 退貨_數量欄].fillna(0).astype(int)
    new_stock = current_stock + return_qty

    # ---- 變動報告（Before → After）----
    有變動的 = is_data_row & (return_qty > 0)
    變動總筆數 = 有變動的.sum()
    最大補貨量 = int(return_qty[is_data_row].max()) if is_data_row.any() else 0

    print(f"📋 本次共修改 {變動總筆數} 筆商品｜最大補貨量: {最大補貨量} 件")
    if 變動總筆數 > 0:
        print(f"{'商品ID':<18} {'規格ID':<18} {'補貨前':>6} → {'補貨後':>6} {'(+補貨量)':>10}")
        print("-" * 70)
        for idx in merged[有變動的].index:
            pid = merged.loc[idx, 主_商品ID欄]
            vid = merged.loc[idx, 主_選項ID欄]
            before = int(current_stock[idx])
            after = int(new_stock[idx])
            qty = after - before
            print(f"{pid:<18} {vid:<18} {before:>6} → {after:>6} (+{qty})")

    # ---- 更新庫存（dry-run 時略過寫檔）----
    merged.loc[is_data_row, 主_庫存欄] = new_stock

    final = merged[main.columns].copy()

    if DRY_RUN:
        print(f"\n  ↳ [DRY-RUN] 不輸出檔案。確認無誤後執行 python main.py 正式補貨。")
    else:
        輸出路徑 = os.path.join(主庫存資料夾, f"已補退貨_{檔名}")
        final.to_excel(輸出路徑, index=False, engine='openpyxl')
        print(f"\n✅ 已輸出: 已補退貨_{檔名}  （總共處理 {len(final)} 筆商品）")

# =============================================
# 3. 結束摘要
# =============================================
print("\n" + "=" * 55)
if DRY_RUN:
    print("  ✅ [DRY-RUN] 預覽完成，未產生任何輸出檔案。")
    print("  確認上方變動正確後，執行以下指令正式補貨：")
    print("     python main.py")
else:
    print("  🎉 全部處理完成！")
    print("  請到「主庫存」資料夾，把「已補退貨_」開頭的檔案")
    print("  上傳到蝦皮批次更新即可。")
print("=" * 55)