# Industrial Processing System API – Kolokvijum 1 (2026)

## Tekst zadatka

Implementirati thread-safe servis **ProcessingSystem** koji simulira obradu industrijskih zadataka (Jobs). Sistem treba da podrži **thread-safe** konkurentnu obradu, asinhrono izvršavanje i event-driven arhitekturu.

---

## Klase

### `Job`
Polja:
- `Guid Id`
- `JobType Type` — dodati enum
- `string Payload`
- `int Priority` — manji broj znači veći prioritet

### `JobHandle`
Polja:
- `Guid Id`
- `Task<int> Result`

### `ProcessingSystem`
- Prima poslove putem metode `Submit(Job job)`
- Vraća `JobHandle`
- Obradu poslova vrši asinhrono koristeći `Task`

---

## Konfiguracija sistema

- Sistem se inicijalizuje iz priloženog **XML konfiguracionog fajla**. Za `payload` smatrati da je uvek u istom i validnom formatu.
- Na osnovu konfiguracije:
  - kreirati odgovarajući broj worker niti
  - inicijalno učitati poslove iz XML-a
- Poslovi sa **većim prioritetom** moraju biti obrađeni pre poslova sa manjim prioritetom.
- `MaxQueueSize` predstavlja maksimalan broj Job-ova u sistemu — odbijaju se novi poslovi ukoliko je queue popunjen.

---

## Obrada poslova

Payload je ulazni parametar i potrebno ga je parsirati.

- **Prime** – izračunavanje broja prostih brojeva do zadate vrednosti (paralelno, proslediti broj niti koje će vršiti obradu). Ograničiti broj niti prilikom parsiranja na interval `[1, 8]`.
  - Payload format: `<gornja_vrednost> <broj_niti>`
- **IO** – simulacija čitanja stanja na određenoj adresi korišćenjem `Thread.Sleep`, vraća se nasumičan broj između 0 i 100.
  - Payload format: `<kašnjenje_u_ms>`

### Idempotentnost
- Isti Job (isti `Id`) **ne sme biti izvršen više puta**.

---

## Event sistem i testiranje

### Događaji
- `JobCompleted`
- `JobFailed`

- Na događaje se pretplatiti koristeći **lambda izraze**.
- Svaki događaj mora **asinhrono** upisati u log fajl:
  ```
  [DateTime] [Status] JobId, Result
  ```

### Retry logika
- Job je **failed** ako mu treba duže od **2 sekunde** da se izvrši.
- Ukoliko dođe do fail-a, pokušati **retry 2 puta**.
- Ukoliko i treći put Job fail-uje, dodati `ABORT` u log fajl za taj Job i ignorisati njegov rezultat.

### Time-independent testiranje
- **Ne koristiti** `Thread.Sleep` za čekanje rezultata.
- Koristiti `TaskCompletionSource`, `SemaphoreSlim` ili slične mehanizme.

---

## Dodatne metode i izveštaj

```csharp
IEnumerable<Job> GetTopJobs(int n)
```
- Vraća prvih N poslova po prioritetu iz trenutno aktivnog reda.

```csharp
Job GetJob(Guid id)
```
- Vraća objekat za zadat ID.

### Periodični izveštaj (svakih 1 minut) — korišćenjem LINQ
- Broj izvršenih poslova po tipu
- Prosečna vreme izvršavanja posla po tipu
- Broj neuspešnih poslova, grupisanih (sortiranih) po tipu

**Format:** XML fajl. Čuvati poslednjih **10 izveštaja** u posebnim fajlovima (kružni bafer — novi izveštaj nakon desetog prepisuje najstariji).

---

## Main program

- Pročitati broj niti iz konfiguracionog fajla.
- Pokrenuti odgovarajući broj niti koje nasumično dodaju nove poslove.
- Svaka nit nasumično dodaje nove poslove.
- Obezbediti:
  - thread-safe pristup sistemu
  - da red ne pređe maksimalnu veličinu iz konfiguracije
- Implementirati odgovarajuće **try-catch** blokove.

---

## Dijagram

```
Industrial Processing System

Main Thread (5 Producer Threads)
        |
     Submit(Job)
        |
  ProcessingSystem
  ┌─────────────────────────┐
  │ Thread-Safe Priority Queue │
  │ MaxQueueSize Limit       │
  │ Idempotency Check        │
  └─────────────────────────┘
        |
      Dequeue
       /    \
Prime Job    IO Job
(CPU-Bound)  (Simulated IO Delay)
       \    /
  JobCompleted Event / JobFailed Event
        |
  TaskCompletionSource (JobHandle)
        |
     Client
   (Await Result)
```

> Zadatak zamisliti kao **producer-consumer**, samo asinhrono i sa prioritetima.

---

## Napomene

- Radi lakše implementacije moguće je dodati određena polja i metode u klase, ali one koje su navedene **moraju da postoje** za maksimalan broj poena.
- Kolokvijum se brani **uživo u subotu (18.4)** na ličnim laptopovima, uz demonstraciju rada i usmeno propitivanje i diskusiju o implementaciji.
- Tačno vreme i mesto odbrane će biti objavljeni sutra tokom dana.
- **Poslati link od GitHub repozitorijuma asistentu do subote u ponoć.**
