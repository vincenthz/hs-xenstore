{-# LANGUAGE OverloadedStrings #-}
import System.Xen.Store

main = do
	(a,b) <- withXS $ \xsh -> do
		v1 <- xsRead xsh "/local/domain/0/name"
		v2 <- tryXS $ xsRead xsh "/local/domain/0/somethingdoesntexist"
		return (v1,v2)
	putStrLn $ show a
	putStrLn $ show b
