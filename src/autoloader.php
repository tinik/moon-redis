<?php

namespace MoonRedis;

spl_autoload_register(function ($class) {
    if (0 !== strpos($class, __NAMESPACE__ . '\\')) {
        return;
    }

    $classPath = __DIR__ . 'autoloader.php/' . str_replace('\\', '/', $class) . '.php';
    if (is_file($classPath)) {
        include $classPath;
    }
}, false, true);
