<?php

for ($i = 10; $i < 99; $i++) {
    $data = [];
    $recordNumbers = random_int(1, 100);
    for ($r = 0; $r < $recordNumbers; $r++) {
        $fractionDigits = random_int(0, 2);
        $totalDigits = random_int($fractionDigits, 5);

        $number = random_int(
            10 ** $fractionDigits,
            10 ** $totalDigits
        );
        $number /= 10 ** $fractionDigits;

        $id = random_int(0, 999);
        $data[] = [$id, $number];
    }

    $f = fopen(
        '.'
        . DIRECTORY_SEPARATOR
        . 'data'
        . DIRECTORY_SEPARATOR
        . "data{$i}.csv",
        'w'
    );
    foreach ($data as $record) {
        fputcsv($f, $record);
    }
    $f = fclose($f);
}
