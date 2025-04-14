# PRD: Migracja bazy danych z Azure SQL Server do PostgreSQL

## 1. Wprowadzenie
Celem projektu jest przeniesienie wszystkich danych z bazy danych SQL Server (umiejscowionej na platformie Azure) do istniejącej bazy danych PostgreSQL, z jednoczesnym zapewnieniem, że żadne dane nie zostaną zgubione i wszystkie operacje zostaną należycie zweryfikowane.

Projekt obejmuje stworzenie ETL Pipeline w Pythonie, który odpowiednio przetworzy dane, dokonując niezbędnych konwersji typów danych oraz walidacji przesyłu danych.

## 2. Główny problem
Migracja danych ze źródłowego SQL Server na platformie Azure do docelowej bazy danych PostgreSQL bez utraty danych i z zachowaniem integralności informacji.

## 3. Cel projektu
- Zapewnienie kompletnego transferu danych (100% rekordów) z bazy SQL Server do PostgreSQL.
- Weryfikacja poprawności transferu danych za pomocą sum kontrolnych, porównania liczby rekordów oraz analizy jakości danych.
- Realizacja jednorazowego transferu, przy czym w trakcie testów wykorzystana zostanie kompletna kopia produkcyjnej bazy danych w środowisku lokalnym.

## 4. Zakres projektu
**W zakresie projektu:**
- Przeniesienie wszystkich danych z SQL Server na Azure do docelowej bazy danych.
- Weryfikacja danych i odpowiednia konwersja typów (np. NVARCHAR → TEXT lub VARCHAR, DATETIME → TIMESTAMP).
- Implementacja ETL Pipeline w Pythonie z wykorzystaniem wybranych bibliotek/frameworków.
- Zapewnienie niezbędnych środków bezpieczeństwa, w tym ochrona przed SQL injection.
- Szczegółowe logowanie operacji, błędów i ostrzeżeń oraz generowanie końcowego raportu migracji w formacie JSON lub MD.

## 5. Struktura projektu
```
.
├── src                     # Główne źródła kodu ETL
│   ├── __init__.py         # (opcjonalnie) Inicjalizacja pakietu
│   ├── config.py           # Konfiguracja połączeń (np. ustawienia bazy danych)
│   ├── extract.py          # Logika ekstrakcji danych z SQL Server
│   ├── transform.py        # Logika transformacji danych (np. konwersje typów)
│   ├── load.py             # Logika załadowania danych do PostgreSQL
│   └── main.py             # Punkt wejścia uruchamiający ETL Pipeline
├── tests                   # Testy jednostkowe i integracyjne (pytest)
│   ├── __init__.py         # (opcjonalnie) Inicjalizacja pakietu testów
│   └── test_etl.py         # Przykładowe testy dla poszczególnych modułów ETL
├── logs                    # Katalog na logi wykonywania procesu
│   └── etl.log             # Przykładowy plik z logami
├── requirements.txt        # Plik z zależnościami
└── README.md               # Ogólny opis projektu, instrukcje uruchomienia, dokumentacja
```

## 6. Wymagania funkcjonalne
- Jednorazowy transfer danych: Cała migracja zostanie wykonana jako jednorazowy transfer pełnej bazy danych.
- Walidacja danych: Wszystkie dostępne metody walidacji zostaną wykorzystane do potwierdzenia kompletności transferu:
  - Sumy kontrolne
  - Porównanie liczby rekordów
  - Analiza jakości danych
- Mapping typów danych: Mapping konwersji typów danych będzie sztywny, np.:
  - NVARCHAR → TEXT lub VARCHAR
  - DATETIME → TIMESTAMP
- Testowanie ETL: Proces migracji zostanie przetestowany na kompletnej kopii produkcyjnej bazy danych w środowisku lokalnym.
- Kontynuacja procesu: Proces migracji będzie kontynuowany pomimo wykrycia błędów, z jednoczesnym rejestrowaniem wszystkich incydentów.
- Logowanie i raportowanie: System musi generować szczegółowe logi oraz końcowy raport migracji dostępny w formacie JSON lub MD.

## 7. Wymagania niefunkcjonalne
- Bezpieczeństwo:
  - Ochrona przed SQL injection.
  - Zastosowanie odpowiednich mechanizmów zabezpieczających transfer danych.
- Wydajność:
  - Brak określonych wymagań dotyczących wydajności procesu migracji.
- Niezawodność:
  - Zapewnienie, że 100% danych zostanie poprawnie przeniesionych z SQL Server do PostgreSQL.
- Monitoring:
  - Szczegółowe logowanie wszystkich operacji, błędów i ostrzeżeń.

## 8. Kryteria sukcesu
- Kompletny przekaz wszystkich danych ze źródła (SQL Server) do docelowej bazy danych (PostgreSQL).
- Potwierdzenie kompletności transferu przy użyciu sum kontrolnych, porównania liczby rekordów oraz analizy jakości danych.
- Wygenerowanie szczegółowego, końcowego raportu migracji.
- System logowania zawierający szczegółowe informacje o przebiegu migracji.

## 9. Ryzyka i zarządzanie błędami
- Ryzyko błędów w konwersji typów danych – konieczność przeprowadzenia dokładnych testów przed migracją.
- Możliwe niezgodności danych – wdrożenie mechanizmów walidacji oraz szczegółowego logowania, które umożliwią diagnozę problemów.
- Potencjalne problemy podczas transferu – proces migracji będzie kontynuowany, a wszelkie błędy zostaną dokładnie zarejestrowane dla późniejszej analizy.

## 10. Podsumowanie
Dokument ten przedstawia szczegółowe wymagania dotyczące procesu migracji danych z Azure SQL Server do PostgreSQL. Projekt skupia się na zapewnieniu kompletności transferu, poprawnej konwersji typów danych oraz wdrożeniu efektywnego ETL Pipeline w Pythonie. Szczegółowe logowanie, walidacja danych oraz końcowy raport migracji są kluczowymi elementami zapewniającymi sukces projektu. 