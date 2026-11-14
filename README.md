# pancake-recorder — Διάβασε πρώτα
Ζωντανή καταγραφή του PancakeSwap Prediction (BNBUSD, BNB Chain) για να δούμε τι έβλεπαν οι κερδισμένοι παίκτες τα τελευταία δευτερόλεπτα πριν ποντάρουν.
Μέρος του project `crypto\pancake-prediction` (βλ. ANALYSIS_winners.md, RESULTS_selection.md εκεί).

## Κατάσταση
| Ημερομηνία | Τι |
|---|---|
| 2026-11-14 | Δοκιμή 5 λεπτών: 87/87 logs και 27/27 πονταρίσματα σε σχέση με το blockchain (PASS). Τιμές: binance_vision, OKX, Bybit, Gate δουλεύουν· binance.com/perp/US μπλοκαρισμένα από ΗΠΑ. |
| 2026-11-14 21:42 UTC | Ξεκίνησε η συνεχής καταγραφή (run 345 λεπτών + cron κάθε 15'). Repo: github.com/nikolaskahris-png/pancake-recorder |

## Πώς τρέχει
GitHub Actions (`.github/workflows/record.yml`): κάθε run κρατά ~5h45m, ένα νέο μπαίνει στην ουρά κάθε 15 λεπτά, άρα σχεδόν συνεχής καταγραφή. Στο τέλος κάθε run τα δεδομένα γίνονται commit στο `data/`.
Χειροκίνητη δοκιμή: Actions → record → Run workflow → minutes = 5.

## Τι γράφεται (`data/ΗΜΕΡΑ/ΩΡΑ_run_είδος.jsonl.gz`)
- `chain`: όλα τα logs του contract και του Chainlink aggregator (block, χρόνος block, θέση στο block, ώρα λήψης)
- `tx`: gas και αποστολέας κάθε πονταρίσματος
- `head`: πότε είδαμε κάθε νέο block
- `pool`: pools + Chainlink κάθε ~1s στα τελευταία 45s πριν το κλείδωμα
- `price`: τιμή BNB ανά 200ms από όσες πηγές απαντούν από ΗΠΑ (Binance μάλλον μπλοκαρισμένη — το health το λέει)
- `health/RUN.json`: μετρητές, σφάλματα, ποιες πηγές τιμών δούλεψαν

## Τι λείπει / ρίσκα
- Οι υπολογιστές του GitHub είναι στις ΗΠΑ: Binance/Bybit/OKX ίσως δεν απαντούν.
- Κενά ~30s ανάμεσα στα runs και όταν το GitHub καθυστερεί.
- Οι όροι του GitHub Actions προτιμούν χρήση για ανάπτυξη λογισμικού· μακροχρόνια συλλογή μπορεί να σταματήσει.
- Δεν βλέπουμε pending συναλλαγές (mempool), μόνο ό,τι μπήκε σε block.
