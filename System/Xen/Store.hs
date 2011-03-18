{-# LANGUAGE OverloadedStrings, DeriveDataTypeable #-}
module System.Xen.Store
	( XsHandle
	, XsError(..)
	, Perm(..)
	, XsPerms(..)
	, XsPath
	, XsData
	, XsWatchCallback
	-- connection handling
	, initiateXS
	, terminateXS
	, withXS
	, tryXS
	-- operations
	, withTransaction
	, xsWrite
	, xsRead
	, xsMkdir
	, xsRm
	, xsDirectory
	, xsGetPerms
	, xsSetPerms
	, xsGetDomainPath
	, xsWatch
	, xsUnwatch
	) where

import Data.Word
import Data.List (intersperse)

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.ByteString.Char8 as BC (pack, unpack)

import Data.Serialize
import Data.IORef
import Data.Typeable

import Control.Concurrent
import Control.Exception (catchJust, tryJust, bracketOnError, bracket, throwIO, Exception(..))
import Control.Applicative ((<$>))
import Control.Monad

import System.IO

import Network.Socket

data Operation =
	  Debug
	| Directory
	| Read
	| GetPerms
	| Watch
	| Unwatch
	| TransactionStart
	| TransactionEnd
	| Introduce
	| Release
	| GetDomainPath
	| Write
	| Mkdir
	| Rm
	| SetPerms
	| WatchEvent
	| Error
	| IsIntroduced
	| Resume
	| SetTarget
	| Restrict
	deriving (Show,Eq,Enum)

-- this represent a xenstore packet header
data PacketHeader = PacketHeader
	{ pTransactionId :: Word32
	, pRequestId     :: Word32
	, pType          :: Operation
	, pLength        :: Word32
	} deriving (Show,Eq)

type PacketData = ByteString

type Packet = (PacketHeader, PacketData)

type XsPath = ByteString
type XsData = ByteString
type XsWatchCallback = XsPath -> IO ()

data Perm = PermNone | PermRead | PermWrite | PermRDWR
	deriving (Show,Eq)

data XsError = ErrorNoEnt | ErrorAgain | ErrorInval | ErrorOther ByteString
	deriving (Show,Eq,Typeable)

instance Exception XsError

data XsPerms = XsPerms
	{ permOwner :: Word32
	, permOther :: Perm
	, permACL   :: [(Word32,Perm)]
	}

instance Serialize Perm where
	put PermNone  = putWord8 $ fromIntegral $ fromEnum 'n'
	put PermRead  = putWord8 $ fromIntegral $ fromEnum 'r'
	put PermWrite = putWord8 $ fromIntegral $ fromEnum 'w'
	put PermRDWR  = putWord8 $ fromIntegral $ fromEnum 'b'
	get           = getWord8 >>= \w -> case toEnum $ fromIntegral w of
		'n' -> return PermNone
		'r' -> return PermRead
		'w' -> return PermWrite
		'b' -> return PermRDWR
		_   -> error "unknown permission"
		

instance Serialize XsPerms where
	put p = do
		let l = map putOne $ (permOwner p, permOther p) : permACL p
		putByteString $ B.intercalate "\0" l
		where putOne (did,perm) = B.concat [BC.pack $ show did, encode perm]
	get = do
		b <- remaining >>= getByteString
		let l = map getOne $ B.split 0 b
		case l of
			(owner, other) : acls -> return $ XsPerms owner other acls
			[]                    -> error "wrong permission format"
		where getOne b = do
			let (did, perm) = B.splitAt (B.length b - 1) b
			case decode perm of
				Left err -> error err
				Right r  -> (read $ BC.unpack did, r)

instance Serialize PacketHeader where
	put p = 
	        (putWord32host $ fromIntegral $ fromEnum $ pType p) >>
	        (putWord32host $ pRequestId p)                      >>
	        (putWord32host $ pTransactionId p)                  >>
	        (putWord32host $ pLength p)
	get = do
		op  <- toEnum . fromIntegral <$> getWord32host
		rid <- getWord32host
		tid <- getWord32host
		len <- getWord32host
		return $ PacketHeader tid rid op len

data XsHandleState = XsHandleState
	{ xshWatchs  :: [((XsPath, XsData), XsWatchCallback)]
	, xshTID     :: Word32
	}

data XsHandle = XsHandle
	{ unXsHandle :: Handle
	, xshGrab    :: MVar Int
	, xshState   :: IORef XsHandleState
	}

initiateXS :: IO XsHandle
initiateXS = bracketOnError (socket AF_UNIX Stream 0) sClose $ \sock -> do
	connect sock (SockAddrUnix "/var/run/xenstored/socket")
	handle <- socketToHandle sock ReadWriteMode
	mvar   <- newMVar 0
	xshs   <- newIORef (XsHandleState [] 0)
	return $ XsHandle handle mvar xshs

terminateXS :: XsHandle -> IO ()
terminateXS = hClose . unXsHandle

withXS :: (XsHandle -> IO a) -> IO a
withXS f = bracket initiateXS terminateXS f

tryXS :: IO a -> IO (Maybe a)
tryXS f = either (const Nothing) Just <$> tryJust (\e -> if e == ErrorNoEnt then Just () else Nothing) f

modifyState xsh f = modifyIORef (xshState xsh) f

registerWatch :: XsHandle -> XsPath -> XsData -> XsWatchCallback -> IO ()
registerWatch xsh p d f =
	modifyState xsh (\st -> st { xshWatchs = ((p,d), f) : xshWatchs st })

unregisterWatch :: XsHandle -> XsPath -> XsData -> IO ()
unregisterWatch xsh p d =
	modifyState xsh (\st -> st { xshWatchs = filter (\z -> (p,d) /= fst z) $ xshWatchs st })

setTransaction xsh tid = modifyState xsh (\st -> st { xshTID = tid })
clearTransaction xsh   = setTransaction xsh 0

{----- packet making -----}

payloadArgs, payloadArgs0 :: [ByteString] -> ByteString
payloadArgs args = B.concat (intersperse "\0" args)
payloadArgs0 args = payloadArgs args `B.append` "\0"

packet :: Operation -> ByteString -> Packet
packet op payload = (PacketHeader 0 0 op (fromIntegral $ B.length payload), payload)

packetRead, packetMkdir, packetRm, packetDirectory, packetGetPerms :: XsPath -> Packet
packetRead      path = packet Read $ payloadArgs0 [path]
packetDirectory path = packet Directory $ payloadArgs0 [path]
packetGetPerms  path = packet GetPerms $ payloadArgs0 [path]
packetMkdir     path = packet Mkdir $ payloadArgs0 [path]
packetRm        path = packet Rm $ payloadArgs0 [path]

packetWrite, packetWatch, packetUnwatch :: XsPath -> XsData -> Packet
packetWatch   path b = packet Watch $ payloadArgs0 [path,b]
packetUnwatch path b = packet Unwatch $ payloadArgs0 [path,b]
packetWrite   path b = packet Write $ payloadArgs [path,b]

packetGetDomainPath :: Word32 -> Packet
packetGetDomainPath domid = packet GetDomainPath $ payloadArgs0 [BC.pack $ show domid]

packetTransactionStart :: Packet
packetTransactionStart = packet TransactionStart $ payloadArgs0 []

packetTransactionEnd :: Bool -> Packet
packetTransactionEnd commit = packet TransactionEnd $ payloadArgs0 [ if commit then "T" else "F" ]

packetSetPerms :: XsPath -> XsPerms -> Packet
packetSetPerms path perms = packet SetPerms $ payloadArgs0 [path, encode perms]

{----- -----}
rpc :: XsHandle -> Packet -> IO Packet
rpc xsh (hdr, payload) = do
	let h = unXsHandle xsh
	grabbing $ do
		tid <- xshTID <$> readIORef (xshState xsh)
		B.hPut h (encodeHeader tid) >> B.hPut h payload >> hFlush h
		recvPacket h
	where
		encodeHeader tid = encode (hdr { pTransactionId = tid })
		recvPacket h = do
			(Right rhdr) <- decode <$> B.hGet h 16
			rpay         <- B.hGet h (fromIntegral $ pLength rhdr)
			case pType rhdr of
				Error      -> throwIO $ case rpay of
					"ENOENT\0" -> ErrorNoEnt
					"EAGAIN\0" -> ErrorAgain
					"EINVAL\0" -> ErrorInval
					_          -> ErrorOther rpay
				WatchEvent -> do
					callWatch rpay
					recvPacket h
				_          -> return (rhdr, rpay)

		callWatch rpay = case B.split 0 rpay of	
			[p,d] -> do
				w <- xshWatchs <$> readIORef (xshState xsh)
				case lookup (p,d) w of
					Nothing -> return ()
					Just f  -> f p
			_     -> -- ignore bad watchevents
				return ()
	
		grabbing f = do
			modifyMVar (xshGrab xsh) $ \v -> f >>= \r -> return (v+1, r)

withTransaction :: XsHandle -> IO a -> IO a
withTransaction xsh f = toRun
	where
		toRun = do
			xsTransactionStart xsh
			r <- catch f (\e -> xsTransactionEnd xsh False >> throwIO e)
			again <- catchJust (\e -> if e == ErrorAgain then Just () else Nothing)
			                   (xsTransactionEnd xsh True >> return False)
			                   (\_ -> return True)
			if again then toRun else return r

ack ((_, payload)) = when (payload /= "OK") $ do
	error "unexpected reply, expecting OK"

-- | end a transaction.
xsTransactionEnd :: XsHandle -> Bool -> IO ()
xsTransactionEnd xsh b = rpc xsh (packetTransactionEnd b) >>= ack >> clearTransaction xsh

-- | start a new transaction. cannot be nested
xsTransactionStart :: XsHandle -> IO ()
xsTransactionStart xsh = rpc xsh packetTransactionStart >>= setTransaction xsh . read . BC.unpack . snd

-- | write a value to a path in the store.
xsWrite :: XsHandle -> XsPath -> XsData -> IO ()
xsWrite xsh p d = rpc xsh (packetWrite p d) >>= ack

-- |  unwatch a watch
xsUnwatch :: XsHandle -> XsPath -> XsData -> IO ()
xsUnwatch xsh p d = rpc xsh (packetUnwatch p d) >>= ack >> unregisterWatch xsh p d

-- | watch a specific path for change, and execute a specific callback when it's firing
xsWatch :: XsHandle -> XsPath -> XsData -> XsWatchCallback -> IO ()
xsWatch xsh p d f = rpc xsh (packetWatch p d) >>= ack >> registerWatch xsh p d f

-- | read a value from the path in the store.
xsRead :: XsHandle -> XsPath -> IO XsData
xsRead xsh p = snd <$> rpc xsh (packetRead p)

-- | create a path in the store.
xsMkdir :: XsHandle -> XsPath -> IO ()
xsMkdir xsh p = rpc xsh (packetMkdir p) >>= ack

-- | remove a path in the store.
xsRm :: XsHandle -> XsPath -> IO ()
xsRm xsh p = rpc xsh (packetRm p) >>= ack

-- | list entries from this path in the store.
xsDirectory :: XsHandle -> XsPath -> IO [XsPath]
xsDirectory xsh p = B.split 0 . snd <$> rpc xsh (packetDirectory p)

xsGetPerms :: XsHandle -> XsPath -> IO XsPerms
xsGetPerms xsh p = do
	perms <- decode . snd <$> rpc xsh (packetGetPerms p)
	either error return perms

xsSetPerms :: XsHandle -> XsPath -> XsPerms -> IO ()
xsSetPerms xsh p perms = rpc xsh (packetSetPerms p perms) >>= ack

xsGetDomainPath :: XsHandle -> Word32 -> IO XsPath
xsGetDomainPath xsh d = snd <$> rpc xsh (packetGetDomainPath d)
