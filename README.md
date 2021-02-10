Alternative for try working with Redis on Magento 2 



# Install 

Current release installed only by git need to update "repositories" in composer.json 

composer.json 
```
{
  ...
  "repositories": [
      {
          "type": "composer",
          "url": "https://repo.magento.com/"
      },
      {
          "type": "vcs",
          "url":  "git@github.com:tinik/moon-redis.git"
      }
  ],
  ...
}
```

Run command
`composer require tinik/moon-redis`




For magento exist problem he is not working with Composer V2 
`composer self-update --1`
